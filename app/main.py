from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse, HTMLResponse, RedirectResponse, FileResponse
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
print("DOC_TEMPLATE_PATH (env or default):", DOC_TEMPLATE_PATH, "exists:", Path(DOC_TEMPLATE_PATH).exists())

# ---------------------------
# FastAPI app + frontend
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
# Adobe REST helpers (no SDK)
# ---------------------------
ADOBE_HOST = os.getenv("ADOBE_HOST", "https://pdf-services.adobe.io")   # EU: https://pdf-services-ew1.adobe.io
ADOBE_CLIENT_ID = os.getenv("ADOBE_CLIENT_ID")
ADOBE_ACCESS_TOKEN = os.getenv("ADOBE_ACCESS_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

def _check_env():
    if not ADOBE_CLIENT_ID or not ADOBE_ACCESS_TOKEN:
        raise RuntimeError("Missing ADOBE_CLIENT_ID or ADOBE_ACCESS_TOKEN")

def _h_json():
    _check_env()
    return {
        "x-api-key": ADOBE_CLIENT_ID,
        "Authorization": f"Bearer {ADOBE_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

def _h_auth():
    _check_env()
    return {
        "x-api-key": ADOBE_CLIENT_ID,
        "Authorization": f"Bearer {ADOBE_ACCESS_TOKEN}",
    }

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

def adobe_docgen_start(template_asset_id: str, data_asset_id: str) -> str:
    url = f"{ADOBE_HOST}/operation/documentgeneration"
    body = {
        "templateAssetID": template_asset_id,
        "jsonDataAssetID": data_asset_id,
        "outputFormat": "pdf"
    }
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

def download_to(url: str, out_path: str):
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                f.write(chunk)

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
    content = r.content
    headers = {k.lower(): v for k, v in r.headers.items()}
    return content, headers

def save_bytes(path: str, data: bytes):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)

# ---------------------------
# Extract + DocGen pipelines
# ---------------------------
def run_extract(pdf_bytes: bytes, work_prefix: str) -> str:
    a = adobe_assets_create("application/pdf")
    adobe_put_upload(a["uploadUri"], pdf_bytes, "application/pdf")
    loc = adobe_extract_start(a["assetID"])
    info = adobe_poll_job(loc)

    zip_url = _find_download_url(info)
    if not zip_url:
        asset_ids = []
        for key in ("assetID", "result", "assets", "outputs", "output"):
            node = info.get(key)
            if isinstance(node, str):
                asset_ids.append(node)
            elif isinstance(node, dict) and "assetID" in node:
                asset_ids.append(node["assetID"])
            elif isinstance(node, list):
                for item in node:
                    if isinstance(item, dict) and "assetID" in item:
                        asset_ids.append(item["assetID"])
        for aid in asset_ids:
            try:
                meta = adobe_asset_get(aid)
                zip_url = _find_download_url(meta)
                if zip_url:
                    break
            except Exception:
                pass

    if not zip_url:
        print("Adobe Extract job info (no downloadUri found):", json.dumps(info, indent=2)[:5000])
        raise RuntimeError("No downloadUri from Extract job")

    blob, headers = download_bytes(zip_url)
    ctype = headers.get("content-type", "")
    dispo = headers.get("content-disposition", "")
    is_zip = "zip" in ctype or ".zip" in dispo.lower()
    is_json = "json" in ctype or ".json" in dispo.lower()

    out_dir = f"{work_prefix}_extract"
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    if is_zip:
        out_zip = f"{work_prefix}_extract.zip"
        save_bytes(out_zip, blob)
        try:
            with zipfile.ZipFile(out_zip, "r") as z:
                z.extractall(out_dir)
        except zipfile.BadZipFile:
            is_zip = False

    if not is_zip:
        try:
            head = blob[:200].decode("utf-8", "ignore")
            if "<html" in head.lower() or "error" in head.lower():
                print("Adobe download looked like HTML/error. First 500 bytes:\n", head[:500])
                raise RuntimeError("Adobe returned an error page instead of extract output.")
            json.loads(blob.decode("utf-8", "ignore"))
            p = Path(out_dir, "structuredData.json")
            save_bytes(str(p), blob)
        except Exception:
            try:
                with zipfile.ZipFile(io.BytesIO(blob), "r") as z:
                    z.extractall(out_dir)
            except zipfile.BadZipFile:
                preview = blob[:200].decode("utf-8", "ignore")
                raise RuntimeError(f"Extract download was neither ZIP nor JSON. Preview: {preview}")

    p = Path(out_dir, "structuredData.json")
    if not p.exists():
        alt = Path(out_dir, "json", "structuredData.json")
        if alt.exists():
            p = alt
        else:
            candidates = list(Path(out_dir).rglob("structuredData.json"))
            if candidates:
                p = candidates[0]
            else:
                listing = [str(x) for x in Path(out_dir).rglob("*")][:200]
                print("Extracted files:\n", "\n".join(listing))
                raise RuntimeError("structuredData.json not found in Extract output")
    return str(p)

def run_docgen(template_path: str, data_json_path: str, work_prefix: str) -> str:
    t = adobe_assets_create("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    with open(template_path, "rb") as tf:
        adobe_put_upload(t["uploadUri"], tf.read(),
                         "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    d = adobe_assets_create("application/json")
    with open(data_json_path, "rb") as jf:
        adobe_put_upload(d["uploadUri"], jf.read(), "application/json")
    loc = adobe_docgen_start(t["assetID"], d["assetID"])
    info = adobe_poll_job(loc)

    pdf_url = _find_download_url(info)
    if not pdf_url:
        print("DocGen job info (no downloadUri found):", json.dumps(info, indent=2)[:5000])
        raise RuntimeError("No downloadUri from DocGen job")

    out_pdf = f"{work_prefix}_filled.pdf"
    download_to(pdf_url, out_pdf)
    return out_pdf

# ---------------------------
# /upload: FULL FLOW
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

        template_path = DOC_TEMPLATE_PATH
        if Path(template_path).exists():
            final_pdf = run_docgen(template_path, filled_path, pdf_path)
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
