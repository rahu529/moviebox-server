FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY fastplay_service.py ./

EXPOSE 8000
CMD ["sh", "-c", "uvicorn fastplay_service:app --host 0.0.0.0 --port ${PORT}"]
