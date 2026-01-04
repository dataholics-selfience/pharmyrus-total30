FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ALL application files
COPY main.py .
COPY google_patents_crawler.py .
COPY inpi_crawler.py .
COPY wipo_crawler.py .
COPY merge_logic.py .
COPY patent_cliff.py .
COPY celery_app.py .
COPY tasks.py .

# Railway uses PORT env variable
ENV PORT=8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Run FastAPI + Celery Worker in same container
# Railway sets PORT automatically
CMD sh -c "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080} & celery -A celery_app worker --loglevel=info --concurrency=1 -I tasks"

