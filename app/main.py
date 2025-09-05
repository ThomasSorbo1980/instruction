from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import PlainTextResponse, HTMLResponse, RedirectResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware

import os, time, zipfile, tempfile, subprocess, json, io, requests
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

# Download template if missing
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
ADOBE_ACCESS_TOKEN = os.getenv("ADOBE_ACCESS_TOKEN", "")

_token_cache = {"access_token": None, "expires_at": 0}

def get_adobe_access_token():
    if ADOBE_ACCESS_TOKEN:  # fixed token (not recommended)
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
    """
    Start a Document Generation job. Provide either inline_json or data_asset_id.
    """
    url = f"{ADOBE_HOST}/operation/documentgeneration"

    if inline_json is not None:
        body = {
            "assetID": template_asset_id,
            "jsonData": inline_json,
            "outputFormat": "pdf"
        }
    elif data_asset_id is not None:
        body = {
            "assetID": template_asset_id,
            "jsonDataAssetID": data_asset_id,
            "outputFormat": "pdf"
        }
    else:
        raise RuntimeError("Need inline_json or data_asset_id")

    r = requests.post(url, headers=_h_json(), json=body, timeout=60)
    if "Location" not in r.headers:
        raise RuntimeError(f"DocGen start failed: {r.status_code} {r.text}")
    return r.headers["Location"]


def adobe_poll_job(location_url: str, interval_s=2, timeout_s=600) -> dict:
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

def adobe_asset_get(asset_id: str) -> dict:
    url = f"{ADOBE_HOST}/assets/{asset_id}"
    r = requests.get(url, headers=_h_auth(), timeout=60)
    r.raise_for_status()
    return r.json()

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

    blob, headers = download_bytes(zip_url)
    out_dir = f"{work_prefix}_extract"
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    # Try ZIP first
    try:
        with zipfile.ZipFile(io.BytesIO(blob), "r") as z:
            z.extractall(out_dir)
    except zipfile.BadZipFile:
        # fallback to JSON
        p = Path(out_dir, "structuredData.json")
        save_bytes(str(p), blob)

    p = Path(out_dir, "structuredData.json")
    if not p.exists():
        raise RuntimeError("structuredData.json not found in Extract output")
    return str(p)

def run_docgen(template_path: str, data_json_path: str, work_prefix: str) -> str:
    # 1) Upload template DOCX
    t = adobe_assets_create("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    with open(template_path, "rb") as tf:
        adobe_put_upload(
            t["uploadUri"], tf.read(),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )

    # 2) Upload data JSON
    d = adobe_assets_create("application/json")
    with open(data_json_path, "rb") as jf:
        adobe_put_upload(d["uploadUri"], jf.read(), "application/json")

    # 3) Start DocGen (using asset IDs) + poll
    loc = adobe_docgen_start(template_asset_id=t["assetID"], data_asset_id=d["assetID"])
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
# /upload
# ---------------------------
@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Please upload a .pdf file")

    content = await file.read()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        pdf_path = tmp.name
        tmp.write(content)

    try:
        structured_path = run_extract(content, pdf_path)

        filled_path = pdf_path.replace(".pdf", "_filled.json")
        env = os.environ.copy()
        proc = subprocess.run(
            ["python", "ai_normalizer.py", structured_path, filled_path],
            env=env, capture_output=True, text=True
        )
        if proc.returncode != 0:
            raise RuntimeError(f"AI normalizer failed: {proc.stderr}")

        if Path(DOC_TEMPLATE_PATH).exists():
            final_pdf = run_docgen(DOC_TEMPLATE_PATH, filled_path, pdf_path)
            download_name = Path(file.filename).stem + "_filled.pdf"
            return FileResponse(final_pdf, media_type="application/pdf", filename=download_name)
        else:
            return FileResponse(filled_path, media_type="application/json",
                                filename=Path(file.filename).stem + "_processed.json")

    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    finally:
        try:
            Path(pdf_path).unlink(missing_ok=True)
        except Exception:
            pass
