# app/main.py
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI()  # <-- THIS must exist at module (top) level

@app.get("/health", response_class=PlainTextResponse)
def health():
    return "ok"
