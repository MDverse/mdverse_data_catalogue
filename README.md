# MDverse data catalogue

Parquet files and codebook are available on Zenodo: [10.5281/zenodo.7856523](https://doi.org/10.5281/zenodo.7856523)

See [CONTRIBUTING](CONTRIBUTING.md) if you want to contribute to this project.

## Setup your environment

Install [uv](https://docs.astral.sh/uv/getting-started/installation/).

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

## Scrape MD dataset repositories

Currently, we are scraping the following data repositories:

- Zenodo [docs](docs/zenodo.md)
- Figshare [docs](docs/figshare.md)
- ATLAS [docs](docs/atlas.md)
- NOMAD [docs](docs/nomad.md)
- GPCRmd [docs](docs/gpcrmd.md)
- MDDB [docs](docs/mddb.md)

Soon:

- OSF [docs](docs/osf.md)

### Scraping Zenodo, Figshare and OSF

To scrape Zenodo, Figshare and OSF, you need a token. Because these data repositories are generic data repositiries,
you also need a query parameters (in `params/`).

Note that "[false positives](docs/false_positives.md)" are removed during the scraping process.

### Scrape NMRLipids Databank

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

## Automation

You can run scrape all data repositories at once with:

```sh
bash scripts/scrape_all.sh
```

For debugging purpose, you can scrape a small subset of available data:

```sh
bash scripts/scrape_all_debug.sh
```

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
> Be sure, you have have **sufficient** time, bandwidth and disk space to run this command.

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

### Retrieve data

Download parquet files from [Zenodo](https://doi.org/10.5281/zenodo.7856523) to build the database:

```sh
uv run src/download_data.py
```

Files will be downloaded to `data/parquet_files`:

```none
data
└── parquet_files
    ├── datasets.parquet
    ├── files.parquet
    ├── gromacs_gro_files.parquet
    ├── gromacs_mdp_files.parquet
    ├── gromacs_xtc_files.parquet
```

### Build the database

Create the empty database:

```sh
uv run database-create
```

Populate the tables with the data from parquet files:

```sh
uv run database-ingest
```

### Information on the database

Report on the number of rows and columns of the table of the database:

```sh
uv run database-report
```

This will create the file `report.log` with all information.

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
