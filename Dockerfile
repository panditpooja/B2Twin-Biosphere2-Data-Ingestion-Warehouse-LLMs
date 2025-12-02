# Biosphere 2 RAG Application Dockerfile
# Compliant with platform deployment requirements

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (needed for sentence-transformers and faiss)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download embedding model during build (speeds up first startup)
# This caches the model so it doesn't need to download on first run
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')" || echo "Model download will happen at runtime"

# Copy application files (only essential files for deployment)
COPY main.py .
COPY rag_database.py .
COPY simple_interface.py .
COPY api_data_loader.py .

# Copy static folder for Biosphere 3 logo
COPY static/ ./static/

# Create /app/data directory for persistent storage (platform requirement)
RUN mkdir -p /app/data

# Set environment variables
ENV FLASK_APP=main.py
ENV FLASK_ENV=production
ENV PYTHONUNBUFFERED=1

# Platform requirement: Expose port 8080
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/ || exit 1

# Run the application with gunicorn
# Platform requirement: App must listen on 0.0.0.0:8080
CMD gunicorn --bind 0.0.0.0:8080 --workers 2 --timeout 300 --access-logfile - --error-logfile - main:app
