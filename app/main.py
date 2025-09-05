# --- add near the top of app/main.py ---
import requests, time, zipfile, json, os, tempfile
from pathlib import Path

ADOBE_HOST = os.getenv("ADOBE_HOST", "https://pdf-services.adobe.io")
ADOBE_CLIENT_ID = os.getenv("ADOBE_CLIENT_ID")
ADOBE_ACCESS_TOKEN = os.getenv("ADOBE_ACCESS_TOKEN")

def _check_env():
    if not ADOBE_CLIENT_ID or not ADOBE_ACCESS_TOKEN:
        raise RuntimeError("Missing ADOBE_CLIENT_ID or ADOBE_ACCESS_TOKEN")

def _h_json():
    _check_env()
    return {"x-api-key": ADOBE_CLIENT_ID,
            "Authorization": f"Bearer {ADOBE_ACCESS_TOKEN}",
            "Content-Type": "application/json"}

def _h_auth():
    _check_env()
    return {"x-api-key": ADOBE_CLIENT_ID,
            "Authorization": f"Bearer {ADOBE_ACCESS_TOKEN}"}

# 1) Tell Adobe you'll upload a file → get assetID + uploadUri
def adobe_assets_create(media_type: str) -> dict:
    r = requests.post(f"{ADOBE_HOST}/assets",
                      headers=_h_json(),
                      json={"mediaType": media_type},
                      timeout=60)
    r.raise_for_status()
    return r.json()   # { assetID, uploadUri }

# 2) Upload the bytes to uploadUri with PUT (no Adobe auth headers)
def adobe_put_upload(upload_uri: str, data: bytes, media_type: str):
    r = requests.put(upload_uri, data=data,
                     headers={"Content-Type": media_type}, timeout=300)
    r.raise_for_status()

# 3) Start an Extract job with the assetID → get Location to poll
def adobe_extract_start(asset_id: str) -> str:
    body = {
        "assetID": asset_id,
        "elementsToExtract": ["text", "tables", "figures"],
        "includeStyling": True
    }
    r = requests.post(f"{ADOBE_HOST}/operation/extractpdf",
                      headers=_h_json(), json=body, timeout=60)
    if "Location" not in r.headers:
        raise RuntimeError(f"Extract start failed: {r.status_code} {r.text}")
    return r.headers["Location"]  # job status URL

# 4) Poll a job Location until "done" (or "failed")
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

# 5) Download a presigned URL to a file (no Adobe headers needed)
def download_to(url: str, out_path: str):
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(1024*1024):
                f.write(chunk)

# Convenience: all-in-one Extract → return path to structuredData.json
def run_extract(pdf_bytes: bytes, work_prefix: str) -> str:
    asset = adobe_assets_create("application/pdf")
    adobe_put_upload(asset["uploadUri"], pdf_bytes, "application/pdf")
    loc = adobe_extract_start(asset["assetID"])
    info = adobe_poll_job(loc)
    zip_url = info.get("downloadUri")
    if not zip_url:
        raise RuntimeError("No downloadUri from Extract")
    out_zip = f"{work_prefix}_extract.zip"
    download_to(zip_url, out_zip)
    out_dir = f"{work_prefix}_extract"
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out_zip, "r") as z:
        z.extractall(out_dir)
    p = Path(out_dir, "structuredData.json")
    if not p.exists():
        alt = Path(out_dir, "json", "structuredData.json")
        if alt.exists(): p = alt
        else: raise RuntimeError("structuredData.json not found in ZIP")
    return str(p)

# (Optional) Document Generation via REST (template + JSON → PDF)

def adobe_docgen_start(template_asset_id: str, data_asset_id: str) -> str:
    body = {
        "templateAssetID": template_asset_id,
        "jsonDataAssetID": data_asset_id,
        "outputFormat": "pdf"
    }
    r = requests.post(f"{ADOBE_HOST}/operation/documentgeneration",
                      headers=_h_json(), json=body, timeout=60)
    if "Location" not in r.headers:
        raise RuntimeError(f"DocGen start failed: {r.status_code} {r.text}")
    return r.headers["Location"]

def run_docgen(template_path: str, data_json_path: str, work_prefix: str) -> str:
    # upload template
    t = adobe_assets_create("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    adobe_put_upload(t["uploadUri"], open(template_path, "rb").read(),
                     "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    # upload data json
    d = adobe_assets_create("application/json")
    adobe_put_upload(d["uploadUri"], open(data_json_path, "rb").read(), "application/json")
    # start docgen
    loc = adobe_docgen_start(t["assetID"], d["assetID"])
    info = adobe_poll_job(loc)
    pdf_url = info.get("downloadUri")
    if not pdf_url:
        raise RuntimeError("No downloadUri from DocGen")
    out_pdf = f"{work_prefix}_filled.pdf"
    download_to(pdf_url, out_pdf)
    return out_pdf
