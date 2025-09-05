import requests, time, zipfile
from pathlib import Path

ADOBE_HOST = os.getenv("ADOBE_HOST", "https://pdf-services.adobe.io")  # default US; EU: https://pdf-services-ew1.adobe.io
ADOBE_CLIENT_ID = os.getenv("ADOBE_CLIENT_ID")
ADOBE_ACCESS_TOKEN = os.getenv("ADOBE_ACCESS_TOKEN")

def _headers_json():
    if not ADOBE_CLIENT_ID or not ADOBE_ACCESS_TOKEN:
        raise RuntimeError("Missing ADOBE_CLIENT_ID or ADOBE_ACCESS_TOKEN")
    return {
        "x-api-key": ADOBE_CLIENT_ID,
        "Authorization": f"Bearer {ADOBE_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

def _headers_auth_only():
    # For download URLs (S3 presigned) you WON'T need headers; for status you do.
    return {
        "x-api-key": ADOBE_CLIENT_ID,
        "Authorization": f"Bearer {ADOBE_ACCESS_TOKEN}",
    }

def adobe_assets_create(media_type: str) -> dict:
    url = f"{ADOBE_HOST}/assets"
    res = requests.post(url, headers=_headers_json(), json={"mediaType": media_type}, timeout=60)
    res.raise_for_status()
    return res.json()  # { "assetID": "...", "uploadUri": "..." }

def adobe_put_upload(upload_uri: str, data: bytes, media_type: str):
    # Upload directly to the cloud provider using PUT (no Adobe auth headers here)
    r = requests.put(upload_uri, data=data, headers={"Content-Type": media_type}, timeout=300)
    r.raise_for_status()

def adobe_extract_start(asset_id: str) -> str:
    url = f"{ADOBE_HOST}/operation/extractpdf"
    body = {
        "assetID": asset_id,
        "elementsToExtract": ["text", "tables", "figures"],
        "includeStyling": True
    }
    r = requests.post(url, headers=_headers_json(), json=body, timeout=60)
    # Expected 201 + Location header that you poll
    if r.status_code not in (201, 202) or "Location" not in r.headers:
        raise RuntimeError(f"Extract start failed: {r.status_code} {r.text}")
    return r.headers["Location"]  # e.g. https://pdf-services.adobe.io/operation/extractpdf/{jobId}

def adobe_poll_job(location_url: str, interval=2, timeout_s=300) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = requests.get(location_url, headers=_headers_auth_only(), timeout=30)
        r.raise_for_status()
        info = r.json()
        status = (info.get("status") or "").lower()
        if status == "done":
            return info  # contains downloadUri
        if status == "failed":
            raise RuntimeError(f"Adobe job failed: {info}")
        time.sleep(interval)
    raise TimeoutError("Timed out waiting for Adobe job")

def adobe_download(url: str, out_path: str):
    # downloadUri is a presigned S3/Blob URL – no Adobe headers required
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                f.write(chunk)

def run_adobe_extract_to_structured(pdf_bytes: bytes, work_prefix: str) -> str:
    # 1) create asset & upload PDF
    asset = adobe_assets_create(media_type="application/pdf")
    adobe_put_upload(asset["uploadUri"], pdf_bytes, "application/pdf")

    # 2) start extract job
    loc = adobe_extract_start(asset["assetID"])

    # 3) poll until done, then download result (ZIP)
    info = adobe_poll_job(loc, interval=2, timeout_s=600)
    zip_url = info.get("downloadUri")
    if not zip_url:
        raise RuntimeError(f"No downloadUri in job info: {info}")

    out_zip = f"{work_prefix}_extract.zip"
    adobe_download(zip_url, out_zip)

    # 4) unzip and return structuredData.json path
    extract_dir = f"{work_prefix}_extracted"
    Path(extract_dir).mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out_zip, "r") as z:
        z.extractall(extract_dir)
    # structuredData.json may be at root or under /json/
    p = Path(extract_dir, "structuredData.json")
    if not p.exists():
        alt = Path(extract_dir, "json", "structuredData.json")
        if alt.exists():
            p = alt
        else:
            raise RuntimeError("structuredData.json not found in Extract output")
    return str(p)
