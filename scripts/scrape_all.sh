#! /usr/bin/env bash

uv sync
mkdir -p data
uv run scrape-zenodo --output-dir data --query-file params/query.yml
uv run scrape-figshare --output-dir data --query-file params/query.yml
# uv run scrape-osf --output-dir data --query-file params/query.yml
uv run scrape-atlas --output-dir data
uv run scrape-nomad --output-dir data
uv run scrape-gpcrmd --output-dir data
uv run scrape-mddb --output-dir data
