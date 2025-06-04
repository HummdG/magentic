FROM python:3.11-slim
WORKDIR /app

# Speedy, reproducible installs
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source only (tests & data excluded by .dockerignore)
COPY src/ src/

# Default entrypoint (can be overridden in docker-run)
ENTRYPOINT ["python", "-m", "src.utils.main"]