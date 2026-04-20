#!/bin/bash
set -e

# GIF - Reproducible Execution Script
echo "🚀 Initializing Gold Intelligence Framework..."

# 1. Ensure uv is installed
if ! command -v uv &> /dev/null; then
    echo "📦 uv not found. Installing uv locally..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source $HOME/.local/bin/env
fi

# 2. Sync dependencies
echo "🔄 Synchronizing dependencies from uv.lock..."
uv sync

# 3. Check for .env file
if [ ! -f .env ]; then
    echo "⚠️ .env file missing. Creating from .env.example..."
    cp .env.example .env
fi

# 4. Run Pipeline
echo "🏃 Running Pipeline..."
uv run python main.py

echo "✅ Execution finished."
