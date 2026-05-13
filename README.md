# MDverse data catalogue

Parquet files and codebook are available on Zenodo: [10.5281/zenodo.7856523](https://doi.org/10.5281/zenodo.7856523)

See [CONTRIBUTING](CONTRIBUTING.md) if you want to contribute to this project.

## Setup the environment

We use [uv](https://docs.astral.sh/uv/getting-started/installation/)
to manage dependencies and the project software environment.

Clone this repository:

```bash
git clone https://github.com/MDverse/mdverse_data_catalogue.git
```

Move to the new directory:

```bash
cd mdverse_data_catalogue
```

Create a virtual environment:

```bash
uv sync
```

> **Note:** This project requires PyArrow >= 23.0.1. If you encounter errors
> related to parquet files, upgrade PyArrow by running:
>
> ```sh
> uv add --upgrade pyarrow
> ```

## Scrape MD data repositories

Currently, we are scraping the following data repositories:

- [Zenodo](docs/zenodo.md)
- [Figshare](docs/figshare.md)
- [ATLAS](docs/atlas.md)
- [NOMAD](docs/nomad.md)
- [GPCRmd](docs/gpcrmd.md)
- [MDDB](docs/mddb.md)

Soon:

- [OSF](docs/osf.md)
- [NMRlipids](docs/nmrlipids.md)

### Scraping Zenodo, Figshare and OSF

To scrape Zenodo, Figshare and OSF, you need a token. Because these data repositories are generic data repositories,
you also need a query parameters (in `params/`).

Note that "[false positives](docs/false_positives.md)" are removed during the scraping process.

### Automation

You can scrape all data repositories at once with:

```sh
bash scripts/scrape_all.sh
```

For debugging purpose, you can scrape a small subset of available data:

```sh
bash scripts/scrape_all_debug.sh
```

### Get statistics on scraped data

```sh
uv run get-scrapers-stats --dir data
```

The script will recursively search for Parquet files in the specified folder.

Aggregated results are stored in the `data/` folder in `stats_*.tsv` files.

The notebook `notebooks/scraper_stats.ipynb` provides more in-depth analysis and figures.

## Analyze Gromacs mdp and gro files

### Download files

To download Gromacs mdp and gro files, use the following commands:

```bash
uv run scripts/download_files.py --input data/zenodo_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles
```

```bash
uv run scripts/download_files.py --input data/figshare_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles
```

```bash
uv run scripts/download_files.py --input data/osf_files.tsv \
--storage data/downloads/ --type mdp --type gro --withzipfiles
```

Option `--withzipfiles` will also get files packaged in zip archives. It means that the script will first download the entire zip archive and then extract the mdp and gro files.

This step will take a couple of hours to complete.
Depending on the stability of your internet connection and the availability of the data repository servers, the download might fail for a couple of files.
Re-rerun previous commands to resume the download.
Files already retrieved will not be downloaded again.

Expect about 640 GB of data with the `--withzipfiles` option (~ 8800 gro files and 9500 mdp files)

Numbers are indicative only and may vary depend on the time you run this command (databases tend to get bigger and bigger).

### Parse .mdp files

```bash
uv run scripts/parse_mdp_files.py \
--input data/zenodo_files.tsv --input data/figshare_files.tsv --input data/osf_files.tsv \
--storage data/downloads --output data
```

This step will take a couple of seconds to run. Results will be saved in `data/gromacs_mdp_files_info.tsv`.

### Parse .gro files

A rough molecular composition is deduced from the file `params/residue_name.yml`
that contains a partial list of residues names organized in categories *protein*, *lipid*, *nucleic*, *glucid* and *water & ion*.

```bash
uv run scripts/parse_gro_files.py \
--input data/zenodo_files.tsv --input data/figshare_files.tsv --input data/osf_files.tsv \
--storage data/downloads --residues params/residue_names.yml --output data
```

This step will take about 4 hours to run. Results will be saved in `data/gromacs_gro_files_info.tsv`.

### Export to Parquet

Parquet format is a column-based storage format that is supported by many data analysis tools.
It's an efficient data format for large datasets.

```bash
uv run scripts/export_to_parquet.py
```

This step will take a couple of seconds to run. Results will be saved in:

```bash
data/datasets.parquet
data/files.parquet
data/gromacs_gro_files.parquet
data/gromacs_mdp_files.parquet
```

## Run all script

You can run all commands above with the `run_all.sh` script:

```bash
bash run_all.sh
```

> [!WARNING]
> Be sure, you have have **sufficient** time, ba### Re-ingesting simulation data

If you wish to re-ingest data from any of the following tables:

- **TopologyFile**
- **ParameterFile**
- **TrajectoryFile**

You can run these commands:

```sh
uv run src/ingest_topol_files.py
```

or

```sh
uv run src/ingest_param_files.py
```

or

```sh
uv run src/ingest_traj_files.py
ndwidth and disk space to run this command.

## Upload data on Zenodo (for MDverse maintainers only)

*For the owner of the Zenodo record only. Zenodo token requires `deposit:actions` and `deposit:write` scopes.*

Update metadata:

```bash
uv run scripts/upload_datasets_to_zenodo.py --record 7856524 --metadata params/zenodo_metadata.json
```

Update files:

```bash
uv run scripts/upload_datasets_to_zenodo.py --record 7856524 \
--file data/datasets.parquet \
--file data/files.parquet \
--file data/gromacs_gro_files.parquet \
--file data/gromacs_mdp_files.parquet \
--file docs/data_model_parquet.md
```

> [!NOTE]
> The latest version of the dataset is available with the DOI [10.5281/zenodo.7856523](https://zenodo.org/doi/10.5281/zenodo.7856523).

## Build database

### Create the empty database

```sh
uv run database-create
```

### Ingest datasets (all sources)

Run the datasets parquet for each source first. This populates the `datasets`,
`authors`, and `data_sources` tables.

```sh
uv run src/ingest_data.py /path/to/mdverse_sandbox/data/atlas/2026-02-18/atlas_datasets.parquet
# same pattern applies to figshare, nomad, gpcrmd, mddb, zenodo
```

### Ingest files (all sources)

Once all datasets are ingested, run the files parquet for each source.
This populates the `files` and `file_types` tables.

```sh
uv run src/ingest_data.py /path/to/mdverse_sandbox/data/atlas/2026-02-18/atlas_files.parquet
# same pattern applies to figshare, nomad, gpcrmd, mddb, zenodo
```

### Verify the database

```sh
uv run database-report
```

This will print a summary to the terminal and create a `report.log` file.

### Ingesting a single source

The ingestion script is generic — you can ingest any single parquet file
at any time by passing its path as an argument:

```sh
uv run src/ingest_data.py /path/to/source_datasets.parquet
uv run src/ingest_data.py /path/to/source_files.parquet
```

The script automatically detects whether the file is a datasets or files
parquet based on the filename (`_datasets` or `_files`).

### Re-ingesting simulation data

If you wish to re-ingest data from any of the following tables:

- **TopologyFile**
- **ParameterFile**
- **TrajectoryFile**

You can run these commands:

```sh
uv run src/ingest_topol_files.py
```


or

```sh
uv run src/ingest_param_files.py
```

or

```sh
uv run src/ingest_traj_files.py
```

## Web application and API

See [webapp](webapp/README.md)
