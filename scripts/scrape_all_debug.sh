#! /usr/bin/env bash

# Strict mode
set -euo pipefail

uv sync
mkdir -p tmp
uv run scrape-zenodo --output-dir tmp --query-file params/query_dev.yml --debug
uv run scrape-figshare --output-dir tmp --query-file params/query_dev.yml --debug
# uv run scrape-osf --output-dir tmp --query-file params/query_dev.yml --debug
uv run scrape-atlas --output-dir tmp --debug
uv run scrape-nomad --output-dir tmp --debug
uv run scrape-gpcrmd --output-dir tmp --debug
uv run scrape-mddb --output-dir tmp --debug
