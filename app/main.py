# app/main.py
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI()  # <-- THIS must exist at module (top) level
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"]
)

app.mount("/static", StaticFiles(directory="static"), name="static")
