from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import PlainTextResponse, HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware

import os, time, zipfile, tempfile, subprocess, json, io, requests, uuid, threading
from pathlib import Path

# ---------------------------
# FastAPI + static
# ---------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def index():
    path = os.path.join("static", "index.html")
    if not os.path.exists(path):
        return RedirectResponse(url="/static/index.html")
    with open(path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.get("/health", response_class=PlainTextResponse)
def health():
    return "ok"

# ---------------------------
# Adobe Auth (PDF Services /token)
# ---------------------------
ADOBE_HOST = os.getenv("ADOBE_HOST", "https://pdf-services.adobe.io")  # EU: https://pdf-services-ew1.adobe.io
ADOBE_CLIENT_ID = os.getenv("ADOBE_CLIENT_ID", "")
ADOBE_CLIENT_SECRET = os.getenv("ADOBE_CLIENT_SECRET", "")
ADOBE_ACCESS_TOKEN = os.getenv("ADOBE_ACCESS_TOKEN", "")  # leave unset to auto-fetch

_token_cache = {"access_token": None, "expires_at": 0}

def get_adobe_access_token():
    if ADOBE_ACCESS_TOKEN:
        return ADOBE_ACCESS_TOKEN
    if _token_cache["access_token"] and time.time() < _token_cache["expires_at"] - 60:
        return _token_cache["access_token"]
    if not ADOBE_CLIENT_ID or not ADOBE_CLIENT_SECRET:
        raise RuntimeError("Missing ADOBE_CLIENT_ID or ADOBE_CLIENT_SECRET")
    token_url = f"{ADOBE_HOST}/token"
    resp = requests.post(
        token_url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"client_id": ADOBE_CLIENT_ID, "client_secret": ADOBE_CLIENT_SECRET},
        timeout=30,
    )
    try:
        resp.raise_for_status()
    except Exception:
        print("PDF Services /token error:", resp.status_code, resp.text[:500])
        raise
    tok = resp.json()
    _token_cache["access_token"] = tok["access_token"]
    _token_cache["expires_at"] = time.time() + int(tok.get("expires_in", 3000))
    return _token_cache["access_token"]

def _h_json():
    return {
        "x-api-key": ADOBE_CLIENT_ID,
        "Authorization": f"Bearer {get_adobe_access_token()}",
        "Content-Type": "application/json",
    }

def _h_auth():
    return {
        "x-api-key": ADOBE_CLIENT_ID,
        "Authorization": f"Bearer {get_adobe_access_token()}",
    }

# ---------------------------
# Adobe helpers: assets, extract
# ---------------------------
def adobe_assets_create(media_type: str) -> dict:
    url = f"{ADOBE_HOST}/assets"
    r = requests.post(url, headers=_h_json(), json={"mediaType": media_type}, timeout=60)
    r.raise_for_status()
    return r.json()  # {assetID, uploadUri}

def adobe_put_upload(upload_uri: str, data: bytes, media_type: str):
    r = requests.put(upload_uri, data=data, headers={"Content-Type": media_type}, timeout=300)
    r.raise_for_status()

def adobe_extract_start(asset_id: str) -> str:
    url = f"{ADOBE_HOST}/operation/extractpdf"
    body = {"assetID": asset_id, "elementsToExtract": ["text", "tables"]}
    r = requests.post(url, headers=_h_json(), json=body, timeout=60)
    if "Location" not in r.headers:
        raise RuntimeError(f"Extract start failed: {r.status_code} {r.text}")
    return r.headers["Location"]

def adobe_poll_job(location_url: str, interval_s=2, timeout_s=900) -> dict:
    end = time.time() + timeout_s
    while time.time() < end:
        r = requests.get(location_url, headers=_h_auth(), timeout=30)
        r.raise_for_status()
        info = r.json()
        st = (info.get("status") or "").lower()
        if st == "done":
            return info
        if st == "failed":
            raise RuntimeError(f"Adobe job failed: {info}")
        time.sleep(interval_s)
    raise TimeoutError("Timed out waiting for Adobe job")

def _find_download_url(obj):
    if isinstance(obj, dict):
        if "downloadUri" in obj and obj["downloadUri"]:
            return obj["downloadUri"]
        for v in obj.values():
            u = _find_download_url(v)
            if u: return u
    elif isinstance(obj, list):
        for v in obj:
            u = _find_download_url(v)
            if u: return u
    return None

def download_bytes(url: str):
    r = requests.get(url, stream=True, timeout=300)
    r.raise_for_status()
    return r.content, {k.lower(): v for k, v in r.headers.items()}

def save_bytes(path: str, data: bytes):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)

def run_extract_to_structured(pdf_bytes: bytes, work_prefix: str) -> str:
    a = adobe_assets_create("application/pdf")
    adobe_put_upload(a["uploadUri"], pdf_bytes, "application/pdf")
    loc = adobe_extract_start(a["assetID"])
    info = adobe_poll_job(loc)
    zip_url = _find_download_url(info)
    if not zip_url:
        raise RuntimeError("No downloadUri from Extract job")
    blob, _ = download_bytes(zip_url)
    out_dir = f"{work_prefix}_extract"
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    # Try ZIP first; else JSON
    try:
        with zipfile.ZipFile(io.BytesIO(blob), "r") as z:
            z.extractall(out_dir)
    except zipfile.BadZipFile:
        save_bytes(str(Path(out_dir, "structuredData.json")), blob)
    p = Path(out_dir, "structuredData.json")
    if not p.exists():
        raise RuntimeError("structuredData.json not found in Extract output")
    return str(p)

# ---------------------------
# Background job machinery (JSON result only)
# ---------------------------
JOBS = {}  # job_id -> {"status": "queued|running|done|error", "json": dict|None, "error": str|None}
JOBS_LOCK = threading.Lock()

def process_job(job_id: str, src_pdf_path: str):
    with JOBS_LOCK:
        JOBS[job_id]["status"] = "running"
    try:
        # 1) Extract
        structured_path = run_extract_to_structured(Path(src_pdf_path).read_bytes(), src_pdf_path)

        # 2) Normalize (pass original PDF path for vision assist)
        filled_path = src_pdf_path.replace(".pdf", "_filled.json")
        env = os.environ.copy()
        proc = subprocess.run(
            ["python", "ai_normalizer.py", structured_path, filled_path, src_pdf_path],
            env=env, capture_output=True, text=True
        )
        if proc.returncode != 0:
            raise RuntimeError(f"AI normalizer failed: {proc.stderr}")

        # 3) Load normalized JSON and store in memory
        with open(filled_path, "r", encoding="utf-8") as jf:
            data_obj = json.load(jf)

        with JOBS_LOCK:
            JOBS[job_id].update({"status": "done", "json": data_obj})

    except Exception as e:
        with JOBS_LOCK:
            JOBS[job_id].update({"status": "error", "error": str(e)})
    finally:
        try:
            Path(src_pdf_path).unlink(missing_ok=True)
        except Exception:
            pass

# ---------------------------
# Routes
# ---------------------------
@app.post("/upload")
async def upload(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Please upload a .pdf file")
    # save the PDF to temp
    content = await file.read()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        pdf_path = tmp.name
        tmp.write(content)
    # create job and start background processing
    job_id = str(uuid.uuid4())
    with JOBS_LOCK:
        JOBS[job_id] = {"status": "queued", "json": None, "error": None}
    background_tasks.add_task(process_job, job_id, pdf_path)
    return {"job_id": job_id}

@app.get("/result/{job_id}")
def get_result(job_id: str):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    if job["status"] in ("queued", "running"):
        return {"status": job["status"]}
    if job["status"] == "error":
        return JSONResponse({"status": "error", "error": job["error"]}, status_code=500)
    # done -> return the JSON object directly
    return JSONResponse(job["json"])
