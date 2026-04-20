# Use a slim Python image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/usr/local/venv \
    AIRFLOW_HOME=/usr/local/airflow \
    DUCKDB_PATH=/app/gold_dbt/data/gold_market.duckdb \
    DBT_TARGET=dev \
    ENVIRONMENT=local

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set work directory
WORKDIR /app

# Create required directories (for Airflow dags)
RUN mkdir -p /usr/local/airflow/dags

# Copy project files
COPY pyproject.toml .
COPY uv.lock .

# Install dependencies using uv
RUN uv sync --no-dev

# Copy the application code
COPY . .

# Expose port for Streamlit
EXPOSE 8501

# Default command: Start the Dashboard (Standard for Cloud Run)
CMD ["uv", "run", "streamlit", "run", "dashboard.py", "--server.port", "8501", "--server.address", "0.0.0.0"]
