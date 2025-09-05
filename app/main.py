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
DOC_TEMPLATE_URL = os.getenv("DOC_TEMPLATE_URL")  # optional: direct URL to download template on startup

print("CWD:", os.getcwd())
print("Resolved DEFAULT_TEMPLATE:", DEFAULT_TEMPLATE)
print("DOC_TEMPLATE_PATH (env or default):", DOC_TEMPLATE_PATH, "exists:", Path(DOC_TEMPLATE_PATH).exists())

# Optionally download the template at startup if it's missing and a URL is provided
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
        print("Downloaded template to:", DOC_TEMPLATE_PATH, "size:", Path(DOC_TEMPLATE_PATH).stat().st_size)
    except Exception as e:
        print("Template download failed:", e)
print("After ensure_template -> exists:", Path(DOC_TEMPLATE_PATH).exists())

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

# Optional: download the template to verify path in browser
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
ADOBE_HOST = os.getenv("ADOBE_HOST", "https://pdf-services.adobe.io")  # EU: https://pdf-services-ew1.adobe.io
ADOBE_CLIENT_ID = os.getenv("ADOBE_CLIENT_ID", "")
ADOBE_CLIENT_SECRET = os.getenv("ADOBE_CLIENT_SECRET", "")
# Optional: if you paste a token for testing, we’ll use it; otherwise we’ll fetch from /token:
ADOBE_ACCESS_TOKEN = os.getenv("ADOBE_ACCESS_TOKEN", "")

_token_cache = {"access_token": None, "expires_at": 0}

def get_adobe_access_token():
    """PDF Services tokens come from <ADOBE_HOST>/token, not IMS."""
    if ADOBE_ACCESS_TOKEN:
        return ADOBE_ACCESS_TOKEN

    if _token_cache["access_token"] and time.time() < _token_cache["expires_at"] - 60:
        return _token_cache["access_token"]

    if not ADOBE_CLIENT_ID or not ADOBE_CLIENT_SECRET:
        raise RuntimeError("Missing ADOBE_CLIENT_ID or ADOBE_CLIENT_SECRET")

    # Per docs: form-encoded body with client_id and client_secret
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
    # token response looks like {"access_token":"...","expires_in":86400}
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
# Extract + DocGen pipelines
# ---------------------------
def run_extract(pdf_bytes: bytes, work_prefix: str) -> str:
    # 1) upload PDF
    a = adobe_assets_create("application/pdf")
    adobe_put_upload(a["uploadUri"], pdf_bytes, "application/pdf")
    # 2) start + poll
    loc = adobe_extract_start(a["assetID"])
    info = adobe_poll_job(loc)
    # 3) find download URL (various response shapes)
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

    # 4) download (zip or json)
    blob, headers = download_bytes(zip_url)
    ctype = headers.get("content-type", "")
    dispo = headers.get("content-disposition", "")
    is_zip = "zip" in ctype or ".zip" in dispo.lower()

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
        # Try JSON or a zip without headers
        head = blob[:200].decode("utf-8", "ignore")
        if "<html" in head.lower() and "error" in head.lower():
            print("Adobe download looked like HTML/error. First 500 bytes:\n", head[:500])
            raise RuntimeError("Adobe returned an error page instead of extract output.")
        try:
            # JSON directly
            json.loads(blob.decode("utf-8", "ignore"))
            p = Path(out_dir, "structuredData.json")
            save_bytes(str(p), blob)
        except Exception:
            # maybe a zip w/o headers
            try:
                with zipfile.ZipFile(io.BytesIO(blob), "r") as z:
                    z.extractall(out_dir)
            except zipfile.BadZipFile:
                preview = blob[:200].decode("utf-8", "ignore")
                raise RuntimeError(f"Extract download was neither ZIP nor JSON. Preview: {preview}")

    # 5) locate structuredData.json
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
    # upload template
    t = adobe_assets_create("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    with open(template_path, "rb") as tf:
        adobe_put_upload(t["uploadUri"], tf.read(),
                         "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    # upload json
    d = adobe_assets_create("application/json")
    with open(data_json_path, "rb") as jf:
        adobe_put_upload(d["uploadUri"], jf.read(), "application/json")
    # start + poll
    loc = adobe_docgen_start(t["assetID"], d["assetID"])
    info = adobe_poll_job(loc)

    pdf_url = _find_download_url(info)
    if not pdf_url:
        print("DocGen job info (no downloadUri found):", json.dumps(info, indent_2)[:5000])  # type: ignore
        raise RuntimeError("No downloadUri from DocGen job")

    out_pdf = f"{work_prefix}_filled.pdf"
    download_to(pdf_url, out_pdf)
    return out_pdf

def download_to(url: str, out_path: str):
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                f.write(chunk)

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
        # 1) Adobe Extract
        structured_path = run_extract(content, pdf_path)

        # 2) AI normalizer
        filled_path = pdf_path.replace(".pdf", "_filled.json")
        env = os.environ.copy()
        proc = subprocess.run(
            ["python", "ai_normalizer.py", structured_path, filled_path],
            env=env, capture_output=True, text=True
        )
        if proc.returncode != 0:
            raise RuntimeError(f"AI normalizer failed: {proc.stderr}")

        # 3) DocGen if template exists, else return JSON
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
