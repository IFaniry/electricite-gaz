# Multi-stage lightweight Docker image for sync-tableau (Cloud Run Job)
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install system dependencies if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and default data directory
COPY export_models.py sheets_client.py main.py ./

# Default entrypoint for Cloud Run Jobs
ENTRYPOINT ["python", "main.py"]
