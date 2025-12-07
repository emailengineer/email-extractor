FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    libssl-dev \
    curl \
    dnsutils \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY definition/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY definition/core.py .
COPY definition/worker.py .
COPY engine/api.py .

# Create logs directory
RUN mkdir -p /app/logs

# Expose API port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8000/ || exit 1

# Start application
CMD ["python", "-m", "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]