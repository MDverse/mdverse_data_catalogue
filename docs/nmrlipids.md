# Scrape NMRLipids Databank

Scrape the NMRLipids Databank to extract metadata from molecular dynamics (MD) simulations.

1. Clone the NMRLipids repository

```bash
git clone https://github.com/NMRLipids/BilayerData.git
```

> All metadata are stored in `README.yaml` files under the `Simulations` directory.

1. Extract metadata from simulations

```bash
uv run scripts/scrape_nmrlipids.py \
  --sim-folder /path/to/BilayerData/Simulations
```

This command will:

1. Recursively search for all `README.yaml` files in the `Simulations` directory
2. Parse and normalize MD simulation metadata
3. Inject mandatory metadata fields (source, crawling_date, licence)
4. Validate entries using Pydantic models
5. Save the extracted metadata to Parquet files
