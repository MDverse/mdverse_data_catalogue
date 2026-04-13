"""
Purpose:
    This script transforms data from parquet files into SQLModel objects
    and ingests them into the database. It handles two pipelines:
    - DATASETS pipeline: creates/updates Dataset, Author, and DataSource tables.
    - FILES pipeline:    creates/updates File and FileType tables.

How it works:
    - Parquet files are loaded into pandas DataFrames.
    - Columns are renamed to match the database schema.
    - Data is transformed into SQLModel objects and written to the database.

Performance:
    - All existing records are pre-loaded into in-memory caches before the
      main loop, so existence checks are O(1) dict lookups instead of per-row
      SQL queries.
    - Rows are committed in batches of BATCH_SIZE (default 500) instead of
      one commit per row, reducing transaction overhead by ~99%.
    - Topology, parameter, and trajectory tables are inserted with a single
      session.add_all() call followed by one commit.

Usage:
    The script accepts a single parquet file path as a CLI argument, making
    ingestion granular — you choose exactly which file to ingest, for which
    source, on which date.

    uv run src/ingest_data.py data/atlas/2026-02-18/atlas_datasets.parquet
    uv run src/ingest_data.py data/atlas/2026-02-18/atlas_files.parquet
    uv run src/ingest_data.py data/zenodo/2026-02-18/zenodo_datasets.parquet

    The script detects whether the parquet is a datasets or files file by
    looking for '_datasets' or '_files' in the filename.

    File existence is checked directly against the database (upsert pattern),
    so datasets and files parquets are fully independent — they can be run
    in any order, at any time, as many times as needed without creating duplicates.
"""

import sys
import time
from pathlib import Path
from datetime import timedelta

import pandas as pd
from loguru import logger
from sqlalchemy import Engine
from sqlmodel import Session, select, delete, SQLModel
from tqdm import tqdm

from db_schema import (
    engine,
    Author,
    Dataset,
    DataSource,
    File,
    FileType,
    ParameterFile,
    TopologyFile,
    TrajectoryFile,
)


# ============================================================================
# Logger
# ============================================================================

logger.remove()
logger.add(
    sys.stderr,
    format="{time:MMMM D, YYYY - HH:mm:ss} | <lvl>{level:<8} | {message}</lvl>",
    level="DEBUG",
)
logger.add(
    f"{Path(__file__).stem}.log",
    mode="w",
    format="{time:YYYY-MM-DDTHH:mm:ss} | <lvl>{level:<8} | {message}</lvl>",
    level="DEBUG",
)

BATCH_SIZE = 500  # Number of rows committed per transaction


# ============================================================================
# Helpers
# ============================================================================

def get_or_create(session: Session, Model: SQLModel, attribute: str, value: str, extra: dict = None) -> SQLModel:
    """Return an existing record matching (attribute=value) or create one."""
    result = session.exec(select(Model).where(getattr(Model, attribute) == value)).first()
    if not result:
        data = {attribute: value, **(extra or {})}
        result = Model(**data)
        session.add(result)
        session.flush()  # Assigns the PK without a full commit
    return result


def update_dataset_fields(existing: Dataset, row: pd.Series, fields: list[str]) -> bool:
    """Update fields on an existing Dataset row. Returns True if anything changed."""
    changed = False
    for field in fields:
        new_value = row[field]
        if getattr(existing, field) != new_value:
            setattr(existing, field, new_value)
            changed = True
    return changed


def delete_files_for_update(engine: Engine, dataset_ids: list[int]) -> None:
    """Delete all File (and related) rows for the given dataset IDs."""
    with Session(engine) as session:
        r_files = session.exec(delete(File).where(File.dataset_id.in_(dataset_ids)))
        r_traj  = session.exec(delete(TrajectoryFile))
        r_param = session.exec(delete(ParameterFile))
        r_topo  = session.exec(delete(TopologyFile))
        session.commit()

    logger.info(f"Deleted {r_files.rowcount} File rows for updated datasets.")
    logger.info(f"Deleted {r_traj.rowcount} TrajectoryFile rows.")
    logger.info(f"Deleted {r_param.rowcount} ParameterFile rows.")
    logger.info(f"Deleted {r_topo.rowcount} TopologyFile rows.\n")


# ============================================================================
# Data loading
# ============================================================================

def load_datasets_data(parquet_path: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)

    df = df.rename(columns={
        "dataset_repository_name":    "data_source",
        "dataset_id_in_repository":   "id_in_data_source",
        "dataset_url_in_repository":  "url_in_data_source",
        "date_last_updated":          "date_last_modified",
        "date_last_fetched":          "date_last_crawled",
        "number_of_files":            "file_number",
    })

    df["author"] = df["author_names"].apply(
        lambda x: ",".join(x) if hasattr(x, "__iter__") and not isinstance(x, str) else ""
    )
    df["keywords"] = df["keywords"].apply(
        lambda x: ";".join(x).lower() if hasattr(x, "__iter__") and not isinstance(x, str) else ""
    )

    source_urls = {
        "zenodo":            "https://zenodo.org/",
        "figshare":          "https://figshare.com/",
        "atlas":             "https://www.dsimb.inserm.fr/ATLAS/",
        "nomad":             "https://nomad-lab.eu/",
        "gpcrmd":            "https://www.gpcrmd.org/",
        "mdposit_mmb_node":   "https://mmb.mddbr.eu/",
        "mdposit_inria_node": "https://dynarepo.inria.fr/",
        "mdposit_cineca_node":"https://cineca.mddbr.eu/",
    }
    df["data_source_url"] = df["data_source"].map(source_urls)

    for col in ["doi", "description", "license", "download_number",
                "view_number", "file_number", "date_created", "date_last_modified"]:
        if col not in df.columns:
            df[col] = None

    df["file_number"] = df["file_number"].fillna(0)
    return df


def load_files_data(parquet_path: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)

    df = df.rename(columns={
        "dataset_repository_name":       "data_source",
        "dataset_id_in_repository":      "dataset_id_in_data_source",
        "file_name":                     "name",
        "file_type":                     "type",
        "file_size_in_bytes":            "size_in_bytes",
        "file_md5":                      "md5",
        "file_url_in_repository":        "url",
        "containing_archive_file_name":  "parent_zip_file_name",
    })

    df["is_from_zip_file"] = df["parent_zip_file_name"].notna()
    return df


def load_topology_data(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)[[
        "dataset_origin", "dataset_id", "file_name",
        "atom_number", "has_protein", "has_nucleic",
        "has_lipid", "has_glucid", "has_water_ion",
    ]]
    return df.rename(columns={
        "dataset_origin": "data_source",
        "dataset_id":     "dataset_id_in_data_source",
        "file_name":      "name",
    })


def load_parameter_data(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)[[
        "dataset_origin", "dataset_id", "file_name",
        "dt", "nsteps", "temperature", "thermostat", "barostat", "integrator",
    ]]
    df = df.rename(columns={
        "dataset_origin": "data_source",
        "dataset_id":     "dataset_id_in_data_source",
        "file_name":      "name",
    })
    df["integrator"] = df["integrator"].fillna("undefined")
    return df


def load_trajectory_data(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)[[
        "dataset_origin", "dataset_id", "file_name",
        "atom_number", "frame_number",
    ]]
    return df.rename(columns={
        "dataset_origin": "data_source",
        "dataset_id":     "dataset_id_in_data_source",
        "file_name":      "name",
    })


# ============================================================================
# Dataset / Author / DataSource ingestion
# ============================================================================

DATASET_FIELDS = [
    "doi", "date_created", "date_last_modified", "date_last_crawled",
    "file_number", "url_in_data_source", "title", "description", "keywords",
]


def create_or_update_datasets_authors_origins_tables(
        df: pd.DataFrame,
        engine: Engine,
) -> list[int]:
    """
    Upsert Dataset, Author, and DataSource rows.

    Returns the list of dataset PKs that were newly created or modified —
    the caller uses this to know which file records may need refreshing.
    """
    ids_new, ids_modified, ids_unchanged = [], [], []

    # ── Pre-load lookup caches so we avoid redundant SELECTs ──────────────
    with Session(engine) as session:
        source_cache: dict[str, DataSource] = {
            s.name: s for s in session.exec(select(DataSource)).all()
        }
        author_cache: dict[str, Author] = {
            a.name: a for a in session.exec(select(Author)).all()
        }
        # (dataset_id_in_data_source, data_source_id) → Dataset
        dataset_cache: dict[tuple, Dataset] = {
            (d.id_in_data_source, d.data_source_id): d
            for d in session.exec(select(Dataset)).all()
        }

        for i, (_, row) in enumerate(tqdm(df.iterrows(), total=len(df), desc="Datasets", unit="row")):

            # ── DataSource ─────────────────────────────────────────────────
            src_name = row["data_source"]
            if src_name not in source_cache:
                src = DataSource(name=src_name, url=row["data_source_url"], citation=None, comment=None)
                session.add(src)
                session.flush()
                source_cache[src_name] = src
            origin = source_cache[src_name]

            # ── Authors ────────────────────────────────────────────────────
            author_names = [n.strip() for n in row["author"].split(",") if n.strip()]
            authors = []
            for name in author_names:
                if name not in author_cache:
                    a = Author(name=name, orcid=None)
                    session.add(a)
                    session.flush()
                    author_cache[name] = a
                authors.append(author_cache[name])

            # ── Dataset ────────────────────────────────────────────────────
            cache_key = (row["id_in_data_source"], origin.data_source_id)
            existing = dataset_cache.get(cache_key)

            if not existing:
                ds = Dataset(
                    id_in_data_source=row["id_in_data_source"],
                    doi=row["doi"],
                    date_created=row["date_created"],
                    date_last_modified=row["date_last_modified"],
                    date_last_crawled=row["date_last_crawled"],
                    file_number=row["file_number"],
                    download_number=row["download_number"],
                    view_number=row["view_number"],
                    license=row["license"],
                    url_in_data_source=row["url_in_data_source"],
                    title=row["title"],
                    keywords=row.get("keywords"),
                    description=row.get("description"),
                    data_source=origin,
                )
                ds.author = authors
                session.add(ds)
                session.flush()
                dataset_cache[cache_key] = ds
                ids_new.append(ds.dataset_id)

            else:
                changed = update_dataset_fields(existing, row, DATASET_FIELDS)
                if {a.name for a in existing.author} != {a.name for a in authors}:
                    existing.author = authors
                    changed = True
                if changed:
                    session.add(existing)
                    ids_modified.append(existing.dataset_id)
                else:
                    ids_unchanged.append(existing.dataset_id)

            # ── Batch commit ───────────────────────────────────────────────
            if (i + 1) % BATCH_SIZE == 0:
                session.commit()

        session.commit()  # Flush remainder

    logger.success("Datasets pipeline complete.")
    logger.info(f"  Created : {len(ids_new)}")
    logger.info(f"  Updated : {len(ids_modified)}")
    logger.info(f"  Skipped : {len(ids_unchanged)}")

    return ids_new + ids_modified


# ============================================================================
# File / FileType ingestion
# ============================================================================

def create_files_tables(files_df: pd.DataFrame, engine: Engine) -> None:
    """
    Bulk-upsert File and FileType rows.

    Strategy:
    - Load all existing (dataset_id, file_name) pairs into a set → O(1) existence checks.
    - Load all Dataset PKs into a dict keyed by (id_in_data_source, source_name).
    - Load all FileType PKs into a dict keyed by name.
    - Commit every BATCH_SIZE rows instead of once per row.
    """
    created = skipped = 0

    with Session(engine) as session:

        # ── Pre-load lookup tables ─────────────────────────────────────────
        logger.info("Loading existing records into memory caches…")

        # (data_source_name, id_in_data_source) → (dataset_pk, ...)
        datasets: dict[tuple, Dataset] = {}
        for ds in session.exec(select(Dataset).join(DataSource)).all():
            datasets[(ds.data_source.name, ds.id_in_data_source)] = ds

        file_type_cache: dict[str, FileType] = {
            ft.name: ft for ft in session.exec(select(FileType)).all()
        }

        # Set of (dataset_pk, file_name) already in the DB
        existing_files: set[tuple] = {
            (f.dataset_id, f.name)
            for f in session.exec(select(File.dataset_id, File.name)).all()
        }

        # Cache for zip-parent files: (dataset_pk, file_name) → file_pk
        zip_cache: dict[tuple, int] = {}

        logger.info("Caches loaded. Processing file rows…")

        for i, (_, row) in enumerate(tqdm(files_df.iterrows(), total=len(files_df), desc="Files", unit="row")):

            src_key = (row["data_source"], row["dataset_id_in_data_source"])
            ds = datasets.get(src_key)
            if not ds:
                logger.debug(f"Dataset not found for {src_key}, skipping.")
                continue

            file_key = (ds.dataset_id, row["name"])
            if file_key in existing_files:
                skipped += 1
                continue

            # ── FileType ───────────────────────────────────────────────────
            ft_name = row["type"]
            if ft_name not in file_type_cache:
                ft = FileType(name=ft_name, comment=None)
                session.add(ft)
                session.flush()
                file_type_cache[ft_name] = ft
            ft = file_type_cache[ft_name]

            # ── Parent zip ─────────────────────────────────────────────────
            parent_id = None
            if row["is_from_zip_file"] and pd.notna(row.get("parent_zip_file_name")):
                zip_key = (ds.dataset_id, row["parent_zip_file_name"])
                parent_id = zip_cache.get(zip_key)
                if not parent_id:
                    parent = session.exec(
                        select(File).where(
                            File.dataset_id == ds.dataset_id,
                            File.name == row["parent_zip_file_name"],
                        )
                    ).first()
                    if parent:
                        parent_id = parent.file_id
                        zip_cache[zip_key] = parent_id
                    else:
                        logger.error(
                            f"Parent zip '{row['parent_zip_file_name']}' not found "
                            f"for '{row['name']}' (dataset {ds.dataset_id})."
                        )

            # ── Create File ────────────────────────────────────────────────
            new_file = File(
                name=row["name"],
                size_in_bytes=row["size_in_bytes"],
                md5=row["md5"],
                url=row["url"],
                is_from_zip_file=row["is_from_zip_file"],
                dataset_id=ds.dataset_id,
                file_type_id=ft.file_type_id,
                parent_zip_file_id=parent_id,
            )
            session.add(new_file)
            session.flush()
            existing_files.add(file_key)
            created += 1

            # Cache this file if it could be a zip parent
            if not row["is_from_zip_file"] and ft_name == "zip":
                zip_cache[(ds.dataset_id, row["name"])] = new_file.file_id

            if (i + 1) % BATCH_SIZE == 0:
                session.commit()

        session.commit()

    logger.success("Files pipeline complete.")
    logger.info(f"  Created : {created}")
    logger.info(f"  Skipped : {skipped}")


# ============================================================================
# Topology / Parameter / Trajectory ingestion
# ============================================================================

def _build_file_lookup(session: Session, file_type_name: str) -> dict[tuple, int]:
    """Return {(dataset_id, file_name): file_id} for a given file type."""
    rows = session.exec(
        select(File.dataset_id, File.name, File.file_id)
        .join(FileType)
        .where(FileType.name == file_type_name)
    ).all()
    return {(r.dataset_id, r.name): r.file_id for r in rows}


def _build_dataset_lookup(session: Session) -> dict[tuple, int]:
    """Return {(data_source_name, id_in_data_source): dataset_id}."""
    rows = session.exec(select(Dataset).join(DataSource)).all()
    return {(ds.data_source.name, ds.id_in_data_source): ds.dataset_id for ds in rows}


def create_topology_table(df: pd.DataFrame, engine: Engine) -> None:
    with Session(engine) as session:
        ds_lookup  = _build_dataset_lookup(session)
        file_lookup = _build_file_lookup(session, "gro")

        objects = []
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Topology", unit="row"):
            ds_id = ds_lookup.get((row["data_source"], row["dataset_id_in_data_source"]))
            if not ds_id:
                continue
            file_id = file_lookup.get((ds_id, row["name"]))
            if not file_id:
                continue
            objects.append(TopologyFile(
                file_id=file_id,
                atom_number=row["atom_number"],
                has_protein=row["has_protein"],
                has_nucleic=row["has_nucleic"],
                has_lipid=row["has_lipid"],
                has_glucid=row["has_glucid"],
                has_water_ion=row["has_water_ion"],
            ))

        session.add_all(objects)
        session.commit()

    logger.info(f"TopologyFile rows inserted: {len(objects)}")


def create_parameters_table(df: pd.DataFrame, engine: Engine) -> None:
    with Session(engine) as session:
        ds_lookup  = _build_dataset_lookup(session)
        file_lookup = _build_file_lookup(session, "mdp")

        objects = []
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Parameters", unit="row"):
            ds_id = ds_lookup.get((row["data_source"], row["dataset_id_in_data_source"]))
            if not ds_id:
                continue
            file_id = file_lookup.get((ds_id, row["name"]))
            if not file_id:
                continue
            objects.append(ParameterFile(
                file_id=file_id,
                dt=row["dt"],
                nsteps=row["nsteps"],
                temperature=row["temperature"],
                thermostat=row["thermostat"],
                barostat=row["barostat"],
                integrator=row["integrator"],
            ))

        session.add_all(objects)
        session.commit()

    logger.info(f"ParameterFile rows inserted: {len(objects)}")


def create_trajectory_table(df: pd.DataFrame, engine: Engine) -> None:
    with Session(engine) as session:
        ds_lookup  = _build_dataset_lookup(session)
        file_lookup = _build_file_lookup(session, "xtc")

        objects, missing = [], 0
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Trajectories", unit="row"):
            ds_id = ds_lookup.get((row["data_source"], row["dataset_id_in_data_source"]))
            if not ds_id:
                missing += 1
                continue
            file_id = file_lookup.get((ds_id, row["name"]))
            if not file_id:
                missing += 1
                continue
            objects.append(TrajectoryFile(
                file_id=file_id,
                atom_number=row["atom_number"],
                frame_number=row["frame_number"],
            ))

        session.add_all(objects)
        session.commit()

    logger.info(f"TrajectoryFile rows inserted: {len(objects)}, missing: {missing}")


def create_simulation_tables(engine: Engine) -> None:
    mdp_path = "data/parquet_files/gromacs_mdp_files.parquet"
    gro_path = "data/parquet_files/gromacs_gro_files.parquet"
    xtc_path = "data/parquet_files/gromacs_xtc_files.parquet"

    logger.info("Loading simulation parquet files…")
    topology_df  = load_topology_data(gro_path)
    parameter_df = load_parameter_data(mdp_path)
    trajectory_df = load_trajectory_data(xtc_path)

    logger.info("Creating TrajectoryFile table…")
    create_trajectory_table(trajectory_df, engine)

    logger.info("Creating ParameterFile table…")
    create_parameters_table(parameter_df, engine)

    logger.info("Creating TopologyFile table…")
    create_topology_table(topology_df, engine)

    logger.success("Simulation tables complete.")


# ============================================================================
# Entry point
# ============================================================================

def data_ingestion(parquet_path: str) -> None:
    """
    Ingest a single parquet file into the database.

    Filename must contain '_datasets' or '_files' to select the pipeline.

    Examples:
        uv run src/ingest_data.py data/atlas/2026-02-18/atlas_datasets.parquet
        uv run src/ingest_data.py data/zenodo/2026-02-18/zenodo_files.parquet
    """
    path = Path(parquet_path)

    if not path.exists():
        logger.error(f"File not found: {parquet_path}")
        sys.exit(1)

    if path.suffix != ".parquet":
        logger.error(f"Expected a .parquet file, got: {path.suffix}")
        sys.exit(1)

    logger.info(f"Starting ingestion: {path.name}")
    start = time.perf_counter()

    if "_datasets" in path.name:
        logger.info("Pipeline: DATASETS (Dataset, Author, DataSource)")
        df = load_datasets_data(parquet_path)
        new_or_modified = create_or_update_datasets_authors_origins_tables(df, engine)
        if new_or_modified:
            logger.info(f"{len(new_or_modified)} datasets new/modified — run the matching files parquet next.")
        else:
            logger.info("No new or modified datasets.")

    elif "_files" in path.name:
        logger.info("Pipeline: FILES (File, FileType)")
        df = load_files_data(parquet_path)
        create_files_tables(df, engine)

    else:
        logger.error(f"Cannot determine pipeline from filename: '{path.name}'.")
        logger.error("Filename must contain '_datasets' or '_files'.")
        sys.exit(1)

    elapsed = str(timedelta(seconds=time.perf_counter() - start)).split(".")[0]
    logger.info(f"Ingestion time: {elapsed}")
    logger.success("Done.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: uv run src/ingest_data.py <path_to_parquet>")
        sys.exit(1)

    data_ingestion(sys.argv[1])