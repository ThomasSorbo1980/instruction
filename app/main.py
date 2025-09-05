import os, io, json, tempfile, logging, subprocess
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware

app = FastAPI()
LOG = logging.getLogger("uvicorn")

# CORS (optional, helpful in dev)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static frontend
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Please upload a .pdf file")

    # Save uploaded file to temp
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        pdf_path = tmp.name
        content = await file.read()
        tmp.write(content)

    # ---- Adobe PDF Extract (PLACEHOLDER) ----
    # TODO: Replace this with a real call to Adobe Extract API.
    structured_path = pdf_path.replace(".pdf", ".json")
    with open(structured_path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"Text": "PLACEHOLDER: Replace with Adobe Extract structuredData.json"}, ensure_ascii=False))

    # ---- AI Normalizer ----
    filled_path = pdf_path.replace(".pdf", "_filled.json")
    cmd = ["python", "ai_normalizer.py", structured_path, filled_path]
    env = os.environ.copy()
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    LOG.info(proc.stdout)
    if proc.returncode != 0:
        LOG.error(proc.stderr)
        raise HTTPException(500, "Normalization failed")

    # ---- Adobe Document Generation (PLACEHOLDER) ----
    # TODO: Replace this with real Doc Gen call to produce PDF bytes.
    # For demo, we just return the normalized JSON file.
    return FileResponse(filled_path, media_type="application/json", filename=file.filename.replace(".pdf", "_processed.json"))
