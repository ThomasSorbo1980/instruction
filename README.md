# Drag & Drop PDF → AI Normalize → (Adobe Doc Gen) → Download

This is a web app for **Render**. Users drag-and-drop a PDF, the server:
1. (Placeholder) Runs **Adobe PDF Extract** → `structuredData.json`
2. Uses **ai_normalizer.py** (rules + optional LLM) → `filled_data.json`
3. (Placeholder) Runs **Adobe Document Generation** with your tagged Word template → final PDF
4. Returns the file to the browser

## Local run
```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
# open http://127.0.0.1:8000
