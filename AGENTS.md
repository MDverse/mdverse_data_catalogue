# Instructions for AI agents

## Project overview

MDverse data catalogue (Python package `mdverse`) builds a catalogue of
molecular dynamics (MD) simulation data shared in public data repositories.

The pipeline is:

1. **Scrape** dataset and file metadata from data repositories.
2. **Validate and normalize** metadata with Pydantic models.
3. **Ingest** Parquet files into a relational SQLite database (SQLModel).
4. **Serve** the database through a FastAPI web application.

Project website: <https://mdverse.eu/>

## Code organization

```text
src/mdverse/
├── core/        # Shared utilities (logger)
├── models/      # Pydantic data models: the single source of truth for metadata
├── scrapers/    # One module per data repository + shared network/toolbox code
└── database/    # SQLModel schema, database creation, ingestion, reports, queries
webapp/          # FastAPI web app (Jinja2 templates, static files, Docker)
tests/           # pytest tests (mirrors src/mdverse/ layout)
docs/            # Documentation (one file per data repository, data models, DB schema)
params/          # Query files (YAML) and reference data (file types, residue names)
scripts/         # Legacy and standalone scripts (download, mdp/gro parsing, automation)
notebooks/       # Jupyter notebooks for exploratory analysis and figures
data/            # Output directory (git-ignored, except data/models/)
```

### Data models (`src/mdverse/models/`)

- `dataset.py`: `DatasetCoreMetadata` (repository name, dataset id, URL)
  and `DatasetMetadata` (dates, title, authors, description, DOI, simulation metadata...).
- `file.py`: `FileMetadata` (extends `DatasetCoreMetadata`).
- `simulation.py`: `SimulationMetadata`, `Molecule`, `ForceFieldModel`,
  `Software`, `ExternalIdentifier`.
- `person.py`, `publication.py`: authors and related publications.
- `enums.py`: allowed values (`DatasetSourceName`, `MoleculeType`,
  `ExternalDatabaseName`, `DataType`).
- `scraper.py`: `ScraperContext`, the runtime configuration of a scraper
  (output paths, log file, counters, debug mode).
- `utils.py`: validation (`normalize_datasets_metadata`, `normalize_files_metadata`)
  and Parquet export (`export_list_of_models_to_parquet`).

All models use `ConfigDict(extra="forbid")`: any unexpected field fails validation.

Generate a diagram of the models with:

```sh
uv run draw-pydantic-schema --out-path data/models/pydantic_schema.png
```

### Scrapers (`src/mdverse/scrapers/`)

Supported data repositories: Zenodo, Figshare, ATLAS, NOMAD, GPCRmd, MDDB.
Planned: OSF, NMRlipids (legacy scripts still in `scripts/`).

- Generic repositories (Zenodo, Figshare, OSF) require an API token
  (read from `.env`) and a query file (`params/query.yml`, or
  `params/query_dev.yml` for development). False positive datasets are
  removed during scraping (see `docs/false_positives.md`).
- MD-specific repositories (ATLAS, NOMAD, GPCRmd, MDDB) do not need a query file.
- `network.py`: HTTP helpers (httpx client, retries, HEAD requests,
  remote zip listing, pycurl and Selenium fallbacks).
- `toolbox.py`: text cleaning, date formatting, file exclusion,
  false positive detection, statistics.
- `cli.py`: shared click options.
- `stats.py`: aggregated statistics from Parquet outputs.

Every scraper follows the same workflow in its `main()` function:

1. Create a `ScraperContext` and a logger with `create_logger()`.
2. Create an httpx client and check the connection to the server.
   Exit with `sys.exit(1)` and a `logger.critical()` message on failure.
3. Fetch raw datasets metadata.
4. Extract the relevant fields into dicts that match `DatasetMetadata`.
5. Validate with `normalize_datasets_metadata()`.
6. Export with `export_list_of_models_to_parquet()`.
7. Repeat steps 3 to 6 for files with `FileMetadata`.
8. Print statistics with `print_statistics()`.

When writing a new scraper, copy this structure from an existing one
(for example `nomad.py`), register its entry point in `pyproject.toml`
under `[project.scripts]`, add its name to `DatasetSourceName` if needed,
add it to `scripts/scrape_all.sh`, and document it in `docs/<repository>.md`.

### Database (`src/mdverse/database/`)

SQLModel/SQLite schema in `database.py`, documented in `docs/database_schema.md`
(Mermaid ER diagram). Keep the documentation in sync with any schema change.

### Web application (`webapp/`)

FastAPI app with routers in `webapp/app/frontend/` (controller/service split)
and Jinja2 templates in `webapp/templates/`. Run with:

```sh
uv run fastapi dev webapp/app/main.py
```

## Common commands

```sh
uv sync --dev                                   # Install dependencies
uv run scrape-nomad --output-dir data --debug   # Run one scraper on a small subset
uv run scrape-zenodo --output-dir data --query-file params/query_dev.yml --debug
bash scripts/scrape_all_debug.sh                # Run all scrapers in debug mode
uv run get-scrapers-stats --dir data            # Statistics on scraped data
uv run database-create / database-ingest / database-report / database-query
uv run pytest -m "not network" tests/           # Tests run in CI
uv run pytest tests/                            # All tests, including network ones
uv run prek                                     # All pre-commit hooks
uv run ruff check --fix && uv run ruff format   # Lint and format
```

Use `--debug` when testing scrapers: full runs are long and put load on remote servers.

## Development rules

### General

- Dependencies are managed with uv. Always run tools through `uv run`.
- Never import or add new libraries by yourself. Always ask before adding
  dependencies. Prefer libraries already listed in `pyproject.toml`
  (httpx for HTTP, loguru for logging, click for CLIs, pandas for tables).
- Strictly follow data models and structures. Never bypass Pydantic validation
  or add fields outside of the models. A change to the data model must update
  the models, the tests in `tests/models/` and the related documentation.
- Never hardcode tokens or credentials. Read them from environment variables
  (`.env`: `ZENODO_TOKEN`, `FIGSHARE_TOKEN`, `OSF_TOKEN`...). Never print or
  log them (exclude `token` when dumping `ScraperContext`).
- Never commit data files, logs or large files (pre-commit blocks files
  larger than 500 KB). Write outputs to `data/` or `tmp/`.

### Code style

When writing code:

- Follow PEP 8 and PEP 257.
- Target Python >= 3.12 with modern syntax.
- Respect ruff rules defined in `ruff.toml` (line length 88, many rule sets enabled).
- Keep code consistent, easy to understand yet pythonic.
- Use meaningful variable names.
- Add explanatory comments describing the purpose of the code.
- Use type hints for all function parameters and return values.
- Use `pathlib.Path` for file paths.
- Use the loguru logger passed as parameter (`logger: "loguru.Logger" = loguru.logger`)
  instead of `print()`.
- Pass boolean options as keyword-only arguments (`*, is_in_debug_mode: bool = False`).

When writing functions, always:

- Add descriptive docstrings in NumPy style (`Parameters`, `Returns`, `Raises` sections).
- Use early returns for error conditions.
- Limit size of try / except blocks to the strict minimum.
- Catch specific exceptions, never bare `except` or `except Exception`.
- Keep functions focused and small. Split extraction logic into helper functions.

### Tests

- Write tests with pytest in `tests/`, mirroring the `src/mdverse/` layout.
- Mark tests that need internet access with `@pytest.mark.network`.
  CI runs only `-m "not network"`, so core logic must be testable offline.
- Add tests for every new model, validator or parsing function.

### Git

- Use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
  (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`, `style:`),
  followed by a capitalized description, for example
  `feat: Add organism field in Molecule model`.
- Make sure `uv run prek` and `uv run pytest -m "not network" tests/` pass before committing.
- Commit `uv.lock` together with `pyproject.toml` when dependencies change.

### Documentation

- Keep `README.md` and `docs/` up to date with code changes.
- Markdown must pass markdownlint (config in `.markdownlint.yaml`).

## Code review

When reviewing code, focus on:

### Security issues

- Check for hardcoded secrets, API keys, or credentials.
- Look for SQL injection and XSS vulnerabilities (database queries, Jinja2 templates).
- Verify proper input validation and sanitization.

### Performance red flags

- Spot inefficient loops and algorithmic issues.
- Review caching opportunities for expensive operations.
- Check that HTTP requests reuse a client, respect rate limits and use retries.

### Code quality

- Functions should be focused and appropriately sized.
- Use clear, descriptive naming conventions.
- Ensure proper error handling throughout.
- Check that scraped metadata goes through the Pydantic models.

### Review style

- Be specific and actionable in feedback.
- Always explain the "why" behind recommendations.
- Acknowledge good patterns when you see them.
- Ask clarifying questions when code intent is unclear.

Always prioritize security vulnerabilities and performance issues that could impact users.

Always suggest changes to improve readability.
