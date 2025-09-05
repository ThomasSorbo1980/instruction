FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY static ./static
COPY ai_normalizer.py ./ai_normalizer.py

ENV PORT=10000
EXPOSE 10000
CMD exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT}
