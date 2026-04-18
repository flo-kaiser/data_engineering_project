# Use a slim Python image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/usr/local/venv

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set work directory
WORKDIR /app

# Copy project files
COPY pyproject.toml .
# If uv.lock existed, we'd copy it too
# COPY uv.lock .

# Install dependencies using uv
RUN uv sync --no-dev

# Copy the rest of the application
COPY . .

# Default command (can be overridden for Airflow, dbt, etc.)
CMD ["uv", "run", "python", "main.py"]
