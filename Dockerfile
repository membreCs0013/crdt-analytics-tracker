# syntax=docker/dockerfile:1
# Base image matches local Python 3.13.x (python:3.13-slim).
# Using root user: bind-mounted ./data/nodeX on Docker Desktop for Windows
# requires the container to write to a Windows-hosted NTFS directory.
# Non-root UIDs don't map cleanly to the host UID on Windows bind mounts,
# so we stay root to avoid "permission denied" on data writes.
FROM python:3.13-slim

WORKDIR /app

# Copy requirements first so Docker layer cache is reused when only code changes.
COPY requirements-runtime.txt ./
RUN pip install --no-cache-dir -r requirements-runtime.txt

# Copy only the packages needed at runtime.
COPY crdt/ crdt/
COPY node/ node/

# HEALTHCHECK via stdlib urllib — no curl available in slim image.
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request, sys; \
r=urllib.request.urlopen('http://localhost:5000/health', timeout=2); \
sys.exit(0 if r.status == 200 else 1)"

# Exec-form CMD so Python receives SIGTERM from `docker stop`
# and the signal handler in node/__main__.py triggers the graceful flush.
CMD ["python", "-m", "node"]
