import os, io, json, zipfile, tempfile, logging, subprocess, shutil
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware

# Adobe PDF Services SDK
from adobe.pdfservices.operation.auth.credentials import Credentials
from adobe.pdfservices.operation.execution_context import ExecutionContext
from adobe.pdfservices.operation.io.file_ref import FileRef
from adobe.pdfservices.operation.pdfops.options.extract_pdf import ExtractPDFOptions, ExtractElementType
from adobe.pdfservices.operation.pdfops.extract_pdf_operation import ExtractPDFOperation
from adobe.pdfservices.operation.documentmerge.document_merge_operation import DocumentMergeOperation
from adobe.pdfservices.operation.documentmerge.options.document_merge_options import DocumentMergeOptions, OutputFormat
from adobe.pdfservices.operation.exception.exceptions import ServiceApiException, ServiceUsageException, SDKException

app = FastAPI()
LOG = logging.getLogger("uvicorn")

# ---- CORS (optional) ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Static frontend ----
app.mount("/static", StaticFiles(directory="static"), name="static")

# ---- Environment / paths ----
ADOBE_CREDS_PATH = os.getenv("ADOBE_CREDS_PATH", "pdfservices-api-credentials.json")
DOC_TEMPLATE_PATH = os.getenv("DOC_TEMPLATE_PATH", "Shipping_Instruction_Template_Tagged.docx")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")  # optional for ai_normalizer LLM step

def _adobe_context() -> ExecutionContext:
    """Create an Adobe execution context from the credentials JSON."""
    if not Path(ADOBE_CREDS_PATH).exists():
        raise RuntimeError(f"Adobe creds not found at {ADOBE_CREDS_PATH}")
    creds = Credentials.service_account_credentials_builder().from_file(ADOBE_CREDS_PATH).build()
    return ExecutionContext.create(creds)

def adobe_extract_to_structured_json(pdf_path: str) -> str:
    """
    Calls Adobe PDF Extract and returns a path to structuredData.json extracted from the ZIP.
    """
    ctx = _adobe_context()
    operation = ExtractPDFOperation.create_new()
    operation.set_input(FileRef.create_from_local_file(pdf_path))

    # Configure extraction: text + tables + figures (adjust as needed)
    opts = ExtractPDFOptions.builder() \
        .with_elements_to_extract([ExtractElementType.TEXT, ExtractElementType.TABLES, ExtractElementType.FIGURES]) \
        .build()
    operation.set_options(opts)

    # Run operation → ZIP
    out_zip = pdf_path.replace(".pdf", "_extract.zip")
    result = operation.execute(ctx)
    result.save_as_file(out_zip)

    # Unzip and return structuredData.json
    extract_dir = pdf_path + "_extracted"
    Path(extract_dir).mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out_zip, "r") as z:
        z.extractall(extract_dir)

    # Adobe places JSON at: structuredData.json
    structured_json_path = str(Path(extract_dir, "structuredData.json"))
    if not Path(structured_json_path).exists():
        # Some SDK versions place under /json/structuredData.json
        alt = Path(extract_dir, "json", "structuredData.json")
        if alt.exists():
            structured_json_path = str(alt)
        else:
            raise RuntimeError("structuredData.json not found in Extract output")

    return structured_json_path

def adobe_docgen_pdf(template_path: str, data_json_path: str) -> str:
    """
    Calls Adobe Document Generation to merge JSON into the DOCX template and returns the PDF path.
    """
    if not Path(template_path).exists():
        raise RuntimeError(f"Doc template not found at {template_path}")
    if not Path(data_json_path).exists():
        raise RuntimeError(f"Data JSON not found at {data_json_path}")

    ctx = _adobe_context()
    with open(data_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    options = DocumentMergeOptions(data, OutputFormat.PDF)
    op = DocumentMergeOperation.create_new(options)
    op.set_input(FileRef.create_from_local_file(template_path))

    result = op.execute(ctx)
    out_pdf = str(Path(tempfile.gettempdir(), f"docgen_{Path(data_json_path).stem}.pdf"))
    result.save_as_file(out_pdf)
    return out_pdf

@app.get("/", response_class=HTMLResponse)
def index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Please upload a .pdf file")

    # Save uploaded file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        pdf_path = tmp.name
        content = await file.read()
        tmp.write(content)

    try:
        # 1) Adobe Extract -> structuredData.json
        structured_path = adobe_extract_to_structured_json(pdf_path)

        # 2) AI Normalizer (rules + optional LLM)
        filled_path = pdf_path.replace(".pdf", "_filled.json")
        cmd = ["python", "ai_normalizer.py", structured_path, filled_path]
        env = os.environ.copy()
        if OPENAI_API_KEY:
            env["OPENAI_API_KEY"] = OPENAI_API_KEY
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
        LOG.info(proc.stdout)
        if proc.returncode != 0:
            LOG.error(proc.stderr)
            raise RuntimeError("AI normalization failed")

        # 3) Adobe DocGen -> final PDF
        final_pdf_path = adobe_docgen_pdf(DOC_TEMPLATE_PATH, filled_path)

        # Return filled PDF
        download_name = Path(file.filename).stem + "_filled.pdf"
        return FileResponse(final_pdf_path, media_type="application/pdf", filename=download_name)

    except (ServiceApiException, ServiceUsageException, SDKException) as e:
        LOG.exception("Adobe PDF Services error")
        raise HTTPException(status_code=502, detail=f"Adobe PDF Services error: {e}")
    except Exception as e:
        LOG.exception("Processing error")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Clean temp files (best-effort)
        try:
            for p in [pdf_path]:
                if p and Path(p).exists():
                    Path(p).unlink()
            # Leave intermediate files if you want debugging; otherwise also remove:
            # Path(structured_path).unlink(missing_ok=True)
            # Path(filled_path).unlink(missing_ok=True)
        except Exception:
            pass
