#!/bin/bash
# GIF - Documentation Generator

echo "🚀 Generating Gold Intelligence Framework Documentation..."

# 1. Generate dbt docs
cd gold_dbt
uv run dbt docs generate --target dev

# 2. Create static docs directory if not exists
mkdir -p ../docs

# 3. Move relevant files for static hosting
cp target/index.html ../docs/index.html
cp target/manifest.json ../docs/manifest.json
cp target/catalog.json ../docs/catalog.json

echo "✅ Documentation ready in docs/ directory."
