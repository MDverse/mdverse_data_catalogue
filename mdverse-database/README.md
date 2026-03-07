# MDverse database schema

## Setup environment

We use [uv](https://docs.astral.sh/uv/getting-started/installation/)
to manage dependencies and the project environment.

Clone the GitHub repository:

```sh
git clone git@github.com:MDverse/mdverse_data_schema.git
cd mdverse_data_schema
```

Sync dependencies:

```sh
uv sync
```

## Retrieve data

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

## Build the database

Create the empty database:

```sh
uv run src/create_database.py
```

Populate the tables with the data from parquet files:

```sh
uv run src/ingest_data.py
```

### Information on the database

Report on the number of rows and columns of the table of the database:

```sh
uv run report.py
```

This will create the file `report.log` with the information.

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
