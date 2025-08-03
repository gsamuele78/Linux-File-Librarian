# Linux File Librarian - Enterprise Docker Image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    mediainfo \
    exiftool \
    qpdf \
    ghostscript \
    poppler-utils \
    unrar \
    p7zip-full \
    sqlite3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create application directory
WORKDIR /app

# Create non-root user
RUN groupadd -r librarian && useradd -r -g librarian librarian

# Copy requirements first for better caching
COPY requirements.txt requirements-enhanced.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-enhanced.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY deployment/scripts/ ./scripts/
COPY documentation/ ./documentation/

# Create necessary directories
RUN mkdir -p /app/data/{input,output,cache,logs} && \
    chown -R librarian:librarian /app

# Switch to non-root user
USER librarian

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python3 -c "from src.enterprise.enterprise_system_manager import EnterpriseSystemManager; print('OK')" || exit 1

# Default command
CMD ["python3", "-m", "src.enterprise.professional_orchestrator"]