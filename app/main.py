from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import PlainTextResponse, HTMLResponse, RedirectResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware

import os, time, zipfile, tempfile, subprocess, json, io, requests, uuid, threading
from pathlib import Path

# ---------------------------
# Template path resolution
# ---------------------------
DEFAULT_TEMPLATE = str((Path(__file__).parent / "Shipping_Instruction_Template_Tagged.docx").resolve())
DOC_TEMPLATE_PATH = os.getenv("DOC_TEMPLATE_PATH", DEFAULT_TEMPLATE)
DOC_TEMPLATE_URL = os.getenv("DOC_TEMPLATE_URL")  # optional URL to auto-download at startup

print("CWD:", os.getcwd())
print("Resolved DEFAULT_TEMPLATE:", DEFAULT_TEMPLATE)
print("DOC_TEMPLATE_PATH:", DOC_TEMPLATE_PATH, "exists:", Path(DOC_TEMPLATE_PATH).exists())

def _download_to(path: str, url: str):
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        for chunk in r.iter_content(1024 * 1024):
            f.write(chunk)

if not Path(DOC_TEMPLATE_PATH).exists() and DOC_TEMPLATE_URL:
    try:
        _download_to(DOC_TEMPLATE_PATH, DOC_TEMPLATE_URL)
        print("Downloaded template to:", DOC_TEMPLATE_PATH)
    except Exception as e:
        print("Template download failed:", e)

# ---------------------------
# FastAPI app
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

@app.get("/debug/template")
def debug_template():
    p = Path(DOC_TEMPLATE_PATH)
    if p.exists():
        return FileResponse(
            str(p),
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            filename="Shipping_Instruction_Template_Tagged.docx",
        )
    return JSONResponse({"error": "Template not found", "path": DOC_TEMPLATE_PATH}, status_code=404)

# ---------------------------
# Adobe Auth (PDF Services /token)
# ---------------------------
ADOBE_HOST = os.getenv("ADOBE_HOST", "https://pdf-services.adobe.io")  # or https://pdf-services-ew1.adobe.io for EU
ADOBE_CLIENT_ID = os.getenv("ADOBE_CLIENT_ID", "")
ADOBE_CLIENT_SECRET = os.getenv("ADOBE_CLIENT_SECRET", "")
ADOBE_ACCESS_TOKEN = os.getenv("ADOBE_ACCESS_TOKEN", "")  # leave empty to auto-fetch via /token

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
# Adobe Helpers
# ---------------------------
def adobe_assets_create(media_type: str) -> dict:
    url = f"{ADOBE_HOST}/assets"
    r = requests.post(url, headers=_h_json(), json={"mediaType": media_type}, timeout=60)
    r.raise_for_status()
    return r.json()

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

def adobe_docgen_start(template_asset_id: str, data_asset_id: str = None, inline_json: dict = None) -> str:
    url = f"{ADOBE_HOST}/operation/documentgeneration"
    if inline_json is not None:
        body = {"assetID": template_asset_id, "jsonData": inline_json, "outputFormat": "pdf"}
    elif data_asset_id is not None:
        body = {"assetID": template_asset_id, "jsonDataAssetID": data_asset_id, "outputFormat": "pdf"}
    else:
        raise RuntimeError("adobe_docgen_start: provide inline_json or data_asset_id")
    r = requests.post(url, headers=_h_json(), json=body, timeout=60)
    if "Location" not in r.headers:
        raise RuntimeError(f"DocGen start failed: {r.status_code} {r.text}")
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
            url = _find_download_url(v)
            if url:
                return url
    elif isinstance(obj, list):
        for v in obj:
            url = _find_download_url(v)
            if url:
                return url
    return None

def download_bytes(url: str):
    r = requests.get(url, stream=True, timeout=300)
    r.raise_for_status()
    return r.content, {k.lower(): v for k, v in r.headers.items()}

def save_bytes(path: str, data: bytes):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)

# ---------------------------
# Extract + DocGen
# ---------------------------
def run_extract(pdf_bytes: bytes, work_prefix: str) -> str:
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

def run_docgen_inline(template_path: str, data_json_path: str, work_prefix: str) -> str:
    # Upload template
    t = adobe_assets_create("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    with open(template_path, "rb") as tf:
        adobe_put_upload(t["uploadUri"], tf.read(),
                         "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    # Inline JSON
    with open(data_json_path, "r", encoding="utf-8") as jf:
        data_obj = json.load(jf)
    loc = adobe_docgen_start(template_asset_id=t["assetID"], inline_json=data_obj)
    info = adobe_poll_job(loc)
    pdf_url = _find_download_url(info)
    if not pdf_url:
        raise RuntimeError("No downloadUri from DocGen job")
    out_pdf = f"{work_prefix}_filled.pdf"
    with requests.get(pdf_url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_pdf, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                f.write(chunk)
    return out_pdf

# ---------------------------
# Background job machinery
# ---------------------------
JOBS = {}  # job_id -> {"status": "queued|running|done|error", "result_path": str|None, "error": str|None, "download_name": str|None}
JOBS_LOCK = threading.Lock()

def process_job(job_id: str, src_pdf_path: str, orig_name: str):
    with JOBS_LOCK:
        JOBS[job_id]["status"] = "running"
    try:
        # 1) Extract
        structured_path = run_extract(Path(src_pdf_path).read_bytes(), src_pdf_path)
        # 2) Normalize
        filled_path = src_pdf_path.replace(".pdf", "_filled.json")
        env = os.environ.copy()
        proc = subprocess.run(
            ["python", "ai_normalizer.py", structured_path, filled_path],
            env=env, capture_output=True, text=True
        )
        if proc.returncode != 0:
            raise RuntimeError(f"AI normalizer failed: {proc.stderr}")
        # 3) DocGen (if template exists) else return JSON
        if Path(DOC_TEMPLATE_PATH).exists():
            final_pdf = run_docgen_inline(DOC_TEMPLATE_PATH, filled_path, src_pdf_path)
            download_name = Path(orig_name).stem + "_filled.pdf"
            with JOBS_LOCK:
                JOBS[job_id].update({"status": "done", "result_path": final_pdf, "download_name": download_name})
        else:
            download_name = Path(orig_name).stem + "_processed.json"
            with JOBS_LOCK:
                JOBS[job_id].update({"status": "done", "result_path": filled_path, "download_name": download_name})
    except Exception as e:
        with JOBS_LOCK:
            JOBS[job_id].update({"status": "error", "error": str(e)})
    finally:
        try:
            Path(src_pdf_path).unlink(missing_ok=True)
        except Exception:
            pass

# ---------------------------
# Routes: upload -> job_id, result polling
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
    # create job
    job_id = str(uuid.uuid4())
    with JOBS_LOCK:
        JOBS[job_id] = {"status": "queued", "result_path": None, "error": None, "download_name": None}
    # start background processing
    background_tasks.add_task(process_job, job_id, pdf_path, file.filename)
    # return job id immediately
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
    # done -> stream file
    path = job["result_path"]
    if not path or not Path(path).exists():
        return JSONResponse({"status": "error", "error": "result missing"}, status_code=500)
    # return as a file (pdf or json)
    mime = "application/pdf" if path.endswith(".pdf") else "application/json"
    return FileResponse(path, media_type=mime, filename=job.get("download_name") or Path(path).name)
