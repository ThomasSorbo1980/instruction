from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse, HTMLResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware

import os, time, zipfile, tempfile, subprocess, json
from pathlib import Path
import requests

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
DOC_TEMPLATE_PATH = os.getenv("DOC_TEMPLATE_PATH", "/app/app/Shipping_Instruction_Template_Tagged.docx")
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
    """Step 1: Ask Adobe for an upload URL and assetID for a file we're about to upload."""
    url = f"{ADOBE_HOST}/assets"
    r = requests.post(url, headers=_h_json(), json={"mediaType": media_type}, timeout=60)
    r.raise_for_status()
    return r.json()  # { assetID, uploadUri }

def adobe_put_upload(upload_uri: str, data: bytes, media_type: str):
    """Step 2: Upload raw bytes to the pre-signed uploadUri (no Adobe auth headers)."""
    r = requests.put(upload_uri, data=data, headers={"Content-Type": media_type}, timeout=300)
    r.raise_for_status()

def adobe_extract_start(asset_id: str) -> str:
    """Start an Extract job using the uploaded asset; returns a Location URL to poll."""
    url = f"{ADOBE_HOST}/operation/extractpdf"
    body = {
        "assetID": asset_id,
        # Keep it minimal—some regions/tenants reject unsupported values
        "elementsToExtract": ["text", "tables"]
        # no includeStyling / no figures
    }
    r = requests.post(url, headers=_h_json(), json=body, timeout=60)
    if "Location" not in r.headers:
        raise RuntimeError(f"Extract start failed: {r.status_code} {r.text}")
    return r.headers["Location"]


def adobe_docgen_start(template_asset_id: str, data_asset_id: str) -> str:
    """Start a Document Generation job; returns a Location URL to poll."""
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
    """Step 4: Poll the job Location until done; returns JSON including downloadUri."""
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
    """Step 5: Download presigned URL to a local file (no Adobe headers needed)."""
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                f.write(chunk)
def _find_download_url(obj):
    """Recursively search for a 'downloadUri' anywhere in the job response."""
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
    """Fetch metadata for an asset; some responses give assetID instead of a direct downloadUri."""
    url = f"{ADOBE_HOST}/assets/{asset_id}"
    r = requests.get(url, headers=_h_auth(), timeout=60)
    r.raise_for_status()
    return r.json()  # often includes downloadUri or a list of representations

def run_extract(pdf_bytes: bytes, work_prefix: str) -> str:
    """Extract pipeline → returns path to structuredData.json, handling multiple response shapes."""
    # 1) Create asset & upload PDF
    a = adobe_assets_create("application/pdf")
    adobe_put_upload(a["uploadUri"], pdf_bytes, "application/pdf")

    # 2) Start extract
    loc = adobe_extract_start(a["assetID"])

    # 3) Poll until done
    info = adobe_poll_job(loc)

    # 4) Find a download URL in various response shapes
    zip_url = _find_download_url(info)
    if not zip_url:
        # Some responses provide an assetID instead of downloadUri
        # Try a few likely places:
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
        # Query assets for a downloadUri
        for aid in asset_ids:
            try:
                meta = adobe_asset_get(aid)
                zip_url = _find_download_url(meta)
                if zip_url:
                    break
            except Exception:
                pass

    if not zip_url:
        # Log the job info to help debugging (won't expose to client)
        print("Adobe Extract job info (no downloadUri found):", json.dumps(info, indent=2)[:5000])
        raise RuntimeError("No downloadUri from Extract job")

    # 5) Download ZIP
    out_zip = f"{work_prefix}_extract.zip"
    download_to(zip_url, out_zip)

    # 6) Unzip and locate structuredData.json
    out_dir = f"{work_prefix}_extract"
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out_zip, "r") as z:
        z.extractall(out_dir)

    # structuredData.json might be at root or under /json/
    p = Path(out_dir, "structuredData.json")
    if not p.exists():
        alt = Path(out_dir, "json", "structuredData.json")
        if alt.exists():
            p = alt
        else:
            # As a last resort, search the whole tree
            for candidate in Path(out_dir).rglob("structuredData.json"):
                p = candidate
                break
            if not p.exists():
                # Debug list files to logs
                print("Extracted files:", [str(x) for x in Path(out_dir).rglob("*")][:200])
                raise RuntimeError("structuredData.json not found in Extract ZIP")
    return str(p)


def run_docgen(template_path: str, data_json_path: str, work_prefix: str) -> str:
    """Document Generation pipeline → returns path to final PDF."""
    # Upload template DOCX
    t = adobe_assets_create("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    with open(template_path, "rb") as tf:
        adobe_put_upload(t["uploadUri"], tf.read(),
                         "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    # Upload data JSON
    d = adobe_assets_create("application/json")
    with open(data_json_path, "rb") as jf:
        adobe_put_upload(d["uploadUri"], jf.read(), "application/json")
    # Start DocGen and poll
    loc = adobe_docgen_start(t["assetID"], d["assetID"])
    info = adobe_poll_job(loc)
    pdf_url = info.get("downloadUri")
    if not pdf_url:
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

    # Read the bytes once
    content = await file.read()

    # Save to a temp file to derive a prefix for intermediates
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        pdf_path = tmp.name
        tmp.write(content)

    try:
        # 1) Adobe Extract → structuredData.json
        structured_path = run_extract(content, pdf_path)

        # 2) AI normalizer → filled_data.json
        filled_path = pdf_path.replace(".pdf", "_filled.json")
        env = os.environ.copy()
        # OPENAI_API_KEY is optional; the normalizer will still run rules-only if not set
        proc = subprocess.run(
            ["python", "ai_normalizer.py", structured_path, filled_path],
            env=env, capture_output=True, text=True
        )
        if proc.returncode != 0:
            raise RuntimeError(f"AI normalizer failed: {proc.stderr}")

        # 3) Adobe DocGen → final PDF (if template exists), else return JSON
        template_path = DOC_TEMPLATE_PATH
        if Path(template_path).exists():
            final_pdf = run_docgen(template_path, filled_path, pdf_path)
            download_name = Path(file.filename).stem + "_filled.pdf"
            return FileResponse(final_pdf, media_type="application/pdf", filename=download_name)
        else:
            # Fallback: return normalized JSON
            return FileResponse(filled_path, media_type="application/json",
                                filename=Path(file.filename).stem + "_processed.json")

    except Exception as e:
        # Surface a clean error to the client
        raise HTTPException(status_code=502, detail=str(e))
    finally:
        # Optional cleanup of the original tmp PDF (keep intermediates for debugging if you like)
        try:
            Path(pdf_path).unlink(missing_ok=True)
        except Exception:
            pass
