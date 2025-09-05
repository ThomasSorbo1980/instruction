from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware
import os

# --- FastAPI app instance ---
app = FastAPI()

# --- Middleware for CORS (frontend drag & drop) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Serve static frontend ---
app.mount("/static", StaticFiles(directory="static"), name="static")

# --- Home page: serve the drag-and-drop UI ---
@app.get("/", response_class=HTMLResponse)
def index():
    path = os.path.join("static", "index.html")
    if not os.path.exists(path):
        # Fallback: redirect to /static/index.html if it exists under a different path
        return RedirectResponse(url="/static/index.html")
    with open(path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

# --- Health check route ---
@app.get("/health", response_class=PlainTextResponse)
def health():
    return "ok"

# --- Upload route (temporary stub) ---
@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Please upload a .pdf file")

    # For now, just confirm we received the file.
    return JSONResponse({
        "status": "received",
        "filename": file.filename,
        "size_bytes": len(await file.read())
    })
