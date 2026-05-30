"""Ingest scraped data into the MDverse database."""

import time
from datetime import timedelta
from pathlib import Path

import click
import duckdb
import loguru
import numpy as np
import pandas as pd

from mdverse.core.logger import create_logger

# ============================================================================
# Configuration
# ============================================================================

SOURCE_URLS = {
    "zenodo": "https://zenodo.org/",
    "figshare": "https://figshare.com/",
    "atlas": "https://www.dsimb.inserm.fr/ATLAS/",
    "nomad": "https://nomad-lab.eu/",
    "gpcrmd": "https://www.gpcrmd.org/",
    "mdposit_mmb_node": "https://mmb.mddbr.eu/",
    "mdposit_inria_node": "https://dynarepo.inria.fr/",
    "mdposit_cineca_node": "https://cineca.mddbr.eu/",
}


# ============================================================================
# Connection
# ============================================================================


def get_db_connection(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Get a connection to the DuckDB database.

    Parameters
    ----------
    db_path : Path
        Path to the DuckDB database file.

    Returns
    -------
    duckdb.DuckDBPyConnection
        A connection object to interact with the DuckDB database.
    """
    return duckdb.connect(str(db_path))


# ============================================================================
# DataFrame loaders
# Read only the columns we need; all normalisation done here in pandas
# (vectorised, C speed) before any SQL runs.
# ============================================================================


def load_datasets_df(parquet_path: str) -> pd.DataFrame:
    """Load parquet file into a Pandas dataframe for datasets metadata.

    Parameters
    ----------
    parquet_path : str
        Path to the parquet file containing the metadata.

    Returns
    -------
    pd.DataFrame
        A dataframe containing the datasets metadata.
    """
    df = pd.read_parquet(
        parquet_path,
        columns=[
            "dataset_repository_name",
            "dataset_id_in_repository",
            "doi",
            "date_created",
            "date_last_updated",
            "date_last_fetched",
            "number_of_files",
            "download_number",
            "view_number",
            "license",
            "dataset_url_in_repository",
            "title",
            "author_names",
            "keywords",
            "description",
        ],
    )
    # Rename columns to match the database schema.
    df = df.rename(
        columns={
            "dataset_repository_name": "data_source",
            "dataset_id_in_repository": "id_in_data_source",
            "date_last_updated": "date_last_modified",
            "date_last_fetched": "date_last_crawled",
            "number_of_files": "file_number",
            "dataset_url_in_repository": "url_in_data_source",
            "author_names": "author_list",
        }
    )
    # Flatten author list to a semicolon-separated string.
    df["author_list"] = df["author_list"].apply(
        lambda row: " ;".join(row) if isinstance(row, (list, tuple, np.ndarray)) else ""
    )

    # Flatten keywords list to a semicolon-separated string.
    df["keywords"] = df["keywords"].apply(
        lambda row: " ;".join(row) if isinstance(row, (list, tuple, np.ndarray)) else ""
    )

    for col in ("file_number", "download_number", "view_number"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in ("doi", "license", "description", "url_in_data_source", "title"):
        df[col] = df[col].where(df[col].notna(), other=None)

    df["data_source_url"] = df["data_source"].map(SOURCE_URLS)
    return df


def load_files_df(parquet_path: str) -> pd.DataFrame:
    """Load parquet file into a Pandas dataframe for files metadata.

    Parameters
    ----------
    parquet_path : str
        Path to the parquet file containing the files metadata.

    Returns
    -------
    pd.DataFrame
        A dataframe containing the files metadata.
    """
    df = pd.read_parquet(
        parquet_path,
        columns=[
            "dataset_repository_name",
            "dataset_id_in_repository",
            "file_name",
            "file_url_in_repository",
            "file_size_in_bytes",
            "file_md5",
            "containing_archive_file_name",
            "file_type",
        ],
    )
    df = df.rename(
        columns={
            "dataset_repository_name": "data_source",
            "dataset_id_in_repository": "dataset_id_in_data_source",
            "file_name": "name",
            "file_url_in_repository": "url",
            "file_size_in_bytes": "size_in_bytes",
            "file_md5": "md5",
            "containing_archive_file_name": "parent_zip_file_name",
            "file_type": "file_type_name",
        }
    )
    df["is_from_zip_file"] = df["parent_zip_file_name"].notna().astype(int)
    for col in ("size_in_bytes", "md5", "url", "parent_zip_file_name"):
        df[col] = df[col].where(df[col].notna(), other=None)
    return df


def load_topology_df(path: str) -> pd.DataFrame:
    """Load parquet file into a Pandas dataframe for topology files metadata.

    Parameters
    ----------
    path : str
        Path to the parquet file containing the topology files metadata.

    Returns
    -------
    pd.DataFrame
        A dataframe containing the topology files metadata.
    """
    df = pd.read_parquet(
        path,
        columns=[
            "dataset_origin",
            "dataset_id",
            "file_name",
            "atom_number",
            "has_protein",
            "has_nucleic",
            "has_lipid",
            "has_glucid",
            "has_water_ion",
        ],
    )
    df = df.rename(
        columns={
            "dataset_origin": "data_source",
            "dataset_id": "dataset_id_in_data_source",
            "file_name": "name",
        }
    )
    for col in (
        "has_protein",
        "has_nucleic",
        "has_lipid",
        "has_glucid",
        "has_water_ion",
    ):
        df[col] = df[col].astype(int)
    df["dataset_id_in_data_source"] = df["dataset_id_in_data_source"].astype(str)
    return df


def load_parameter_df(path: str) -> pd.DataFrame:
    """Load parquet file into a Pandas dataframe for parameter files metadata.

    Parameters
    ----------
    path : str
        Path to the parquet file containing the parameter files metadata.

    Returns
    -------
    pd.DataFrame
        A dataframe containing the parameter files metadata.
    """
    df = pd.read_parquet(
        path,
        columns=[
            "dataset_origin",
            "dataset_id",
            "file_name",
            "dt",
            "nsteps",
            "temperature",
            "thermostat",
            "barostat",
            "integrator",
        ],
    )
    df = df.rename(
        columns={
            "dataset_origin": "data_source",
            "dataset_id": "dataset_id_in_data_source",
            "file_name": "name",
        }
    )
    df["integrator"] = df["integrator"].fillna("undefined")
    for col in ("dt", "nsteps", "temperature", "thermostat", "barostat"):
        df[col] = df[col].where(df[col].notna(), other=None)
    df["dataset_id_in_data_source"] = df["dataset_id_in_data_source"].astype(str)
    return df


def load_trajectory_df(path: str) -> pd.DataFrame:
    """Load parquet file into a Pandas dataframe for trajectory files metadata.

    Parameters
    ----------
    path : str
        Path to the parquet file containing the trajectory files metadata.

    Returns
    -------
    pd.DataFrame
        A dataframe containing the trajectory files metadata.
    """
    df = pd.read_parquet(
        path,
        columns=[
            "dataset_origin",
            "dataset_id",
            "file_name",
            "atom_number",
            "frame_number",
        ],
    )
    df = df.rename(
        columns={
            "dataset_origin": "data_source",
            "dataset_id": "dataset_id_in_data_source",
            "file_name": "name",
        }
    )
    df["dataset_id_in_data_source"] = df["dataset_id_in_data_source"].astype(str)
    return df


# ============================================================================
# Pipeline — datasets
#
# All work happens in SQL after registering two DataFrame views:
#   _stage_datasets  — one row per dataset (scalar fields)
#   _stage_authors   — one row per (dataset, author) after exploding the list
#
# Steps:
#   1. Upsert data_sources        — INSERT INTO ... SELECT DISTINCT
#   2. Upsert authors             — INSERT INTO ... SELECT DISTINCT
#   3. Insert new datasets        — INSERT INTO ... SELECT ... WHERE NOT EXISTS
#   4. Update changed datasets    — UPDATE ... FROM view WHERE IS DISTINCT FROM
#   5. Upsert datasets_authors_link — INSERT OR IGNORE INTO ... SELECT
# ============================================================================


def ingest_datasets(
    df: pd.DataFrame,
    db_conn: duckdb.DuckDBPyConnection,
    logger: "loguru.Logger" = loguru.logger,
) -> list[int]:
    """Ingest datasets from the given DataFrame into the database.

    Parameters
    ----------
    df : pd.DataFrame
        A DataFrame containing the datasets metadata to be ingested.
    db_conn : duckdb.DuckDBPyConnection
        A connection to the DuckDB database.
    logger : loguru.Logger, optional
        A logger for logging messages.

    Returns
    -------
    list[int]
    """
    # Build the author-exploded staging frame in pandas (fast, vectorised)
    ds_stage = df.drop(columns=["author_list"]).copy()
    author_stage = (
        df[["id_in_data_source", "data_source", "author_list"]]
        .explode("author_list")
        .rename(columns={"author_list": "author_name"})
        .dropna(subset=["author_name"])
        .assign(author_name=lambda x: x["author_name"].str.strip())
        .query("author_name != ''")
    )

    # Register as DuckDB views — zero-copy, DuckDB reads the Arrow buffer directly
    db_conn.register("_stage_datasets", ds_stage)
    db_conn.register("_stage_authors", author_stage)

    # ── 1. Upsert data_sources ─────────────────────────────────────────────
    db_conn.execute("""
        INSERT INTO data_sources (name, url)
        SELECT DISTINCT data_source, data_source_url
        FROM _stage_datasets
        WHERE data_source NOT IN (SELECT name FROM data_sources)
    """)

    # ── 2. Upsert authors ──────────────────────────────────────────────────
    db_conn.execute("""
        INSERT INTO authors (name)
        SELECT DISTINCT author_name
        FROM _stage_authors
        WHERE author_name NOT IN (SELECT name FROM authors)
    """)

    # ── 3. Insert new datasets ─────────────────────────────────────────────
    n_new = db_conn.execute("""
        INSERT INTO datasets (
            data_source_id, id_in_data_source, url_in_data_source,
            doi, date_created, date_last_modified, date_last_crawled,
            file_number, download_number, view_number,
            license, title, description, keywords
        )
        SELECT
            src.data_source_id,
            s.id_in_data_source,
            s.url_in_data_source,
            s.doi,
            s.date_created,
            s.date_last_modified,
            s.date_last_crawled,
            s.file_number,
            s.download_number,
            s.view_number,
            s.license,
            s.title,
            s.description,
            s.keywords
        FROM _stage_datasets s
        JOIN data_sources src ON src.name = s.data_source
        WHERE NOT EXISTS (
            SELECT 1 FROM datasets d
            WHERE d.data_source_id    = src.data_source_id
              AND d.id_in_data_source = s.id_in_data_source
        )
    """).fetchone()[0]

    # ── 4. Update changed datasets ─────────────────────────────────────────
    # IS DISTINCT FROM handles NULL comparisons correctly (unlike !=)
    n_updated = db_conn.execute("""
        UPDATE datasets d
        SET
            doi                = s.doi,
            date_created       = s.date_created,
            date_last_modified = s.date_last_modified,
            date_last_crawled  = s.date_last_crawled,
            file_number        = s.file_number,
            url_in_data_source = s.url_in_data_source,
            title              = s.title,
            description        = s.description,
            keywords           = s.keywords
        FROM _stage_datasets s
        JOIN data_sources src ON src.name = s.data_source
        WHERE d.data_source_id    = src.data_source_id
          AND d.id_in_data_source = s.id_in_data_source
          AND (
                d.doi                IS DISTINCT FROM s.doi
             OR d.date_created       IS DISTINCT FROM s.date_created
             OR d.date_last_modified IS DISTINCT FROM s.date_last_modified
             OR d.date_last_crawled  IS DISTINCT FROM s.date_last_crawled
             OR d.file_number        IS DISTINCT FROM s.file_number
             OR d.url_in_data_source IS DISTINCT FROM s.url_in_data_source
             OR d.title              IS DISTINCT FROM s.title
             OR d.description        IS DISTINCT FROM s.description
             OR d.keywords           IS DISTINCT FROM s.keywords
          )
    """).fetchone()[0]

    # ── 5. Upsert datasets_authors_link ────────────────────────────────────
    db_conn.execute("""
        INSERT OR IGNORE INTO datasets_authors_link (dataset_id, author_id)
        SELECT d.dataset_id, a.author_id
        FROM _stage_authors sa
        JOIN data_sources src ON src.name              = sa.data_source
        JOIN datasets     d   ON d.data_source_id      = src.data_source_id
                             AND d.id_in_data_source   = sa.id_in_data_source
        JOIN authors      a   ON a.name                = sa.author_name
    """)

    db_conn.commit()
    db_conn.unregister("_stage_datasets")
    db_conn.unregister("_stage_authors")

    # Return all dataset_ids for this source (used by the files pipeline)
    source_names = df["data_source"].unique().tolist()
    all_ids = [
        r[0]
        for r in db_conn.execute(
            """
            SELECT d.dataset_id
            FROM datasets d
            JOIN data_sources src ON src.data_source_id = d.data_source_id
            WHERE src.name IN (SELECT unnest($1::VARCHAR[]))
        """,
            [source_names],
        ).fetchall()
    ]

    n_total = len(all_ids)
    logger.success(
        f"Total datasets: {n_total:,} | new: {n_new:,} | updated: {n_updated:,}"
    )
    return all_ids


# ============================================================================
# Pipeline — files
#
# Two-pass SQL strategy handles parent-zip references without Python loops:
#   Pass 1 — insert all top-level files (is_from_zip_file = 0),
#             including zip archives themselves.
#   Pass 2 — insert zip-child files; parent_zip_file_id resolved via
#             a self-join on files already inserted in pass 1.
# ============================================================================


def _delete_files_for_datasets(
    conn: duckdb.DuckDBPyConnection,
    dataset_ids: list[int],
    logger: "loguru.Logger" = loguru.logger,
) -> None:
    """Delete all files and their simulation children for the given datasets."""
    if not dataset_ids:
        return
    # Remove files for the topology_files table.
    conn.execute(
        """
        DELETE FROM topology_files
        WHERE file_id IN (
            SELECT file_id FROM files
            WHERE dataset_id IN (SELECT unnest($1::INTEGER[]))
        )
        """,
        [dataset_ids],
    )
    # for table in ("topology_files", "parameter_files", "trajectory_files"):

    conn.execute(
        "DELETE FROM files WHERE dataset_id IN (SELECT unnest($1::INTEGER[]))",
        [dataset_ids],
    )
    conn.commit()
    logger.info(f"Deleted existing files for {len(dataset_ids):,} dataset(s).")


def ingest_files(
    df: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
    dataset_ids: list[int],
    logger: "loguru.Logger" = loguru.logger,
) -> None:
    """Bulk-insert file rows for the given dataset_ids."""
    if not dataset_ids:
        logger.info("No datasets to process — skipping file ingestion.")
        return

    # Resolve dataset_id in pandas (vectorised map) then filter to eligible only
    ds_map = {
        (r[0], r[1]): r[2]
        for r in conn.execute("""
            SELECT src.name, d.id_in_data_source, d.dataset_id
            FROM datasets d
            JOIN data_sources src ON src.data_source_id = d.data_source_id
        """).fetchall()
    }
    eligible = set(dataset_ids)
    df = df.copy()
    df["dataset_id"] = df.apply(
        lambda r: ds_map.get((r["data_source"], r["dataset_id_in_data_source"])),
        axis=1,
    )
    df = df[df["dataset_id"].isin(eligible)].reset_index(drop=True)

    if df.empty:
        logger.info("No matching datasets found in files parquet — nothing to ingest.")
        return

    conn.register("_stage_files", df)

    # ── Upsert file_types ──────────────────────────────────────────────────
    conn.execute("""
        INSERT INTO file_types (name)
        SELECT DISTINCT file_type_name
        FROM _stage_files
        WHERE file_type_name IS NOT NULL
          AND file_type_name NOT IN (SELECT name FROM file_types)
    """)

    # ── Pass 1: top-level files (not inside a zip) ─────────────────────────
    conn.execute("""
        INSERT INTO files (
            dataset_id, name, file_type_id, size_in_bytes,
            md5, url, is_from_zip_file, parent_zip_file_id
        )
        SELECT
            s.dataset_id,
            s.name,
            ft.file_type_id,
            s.size_in_bytes,
            s.md5,
            s.url,
            s.is_from_zip_file,
            NULL
        FROM _stage_files s
        JOIN file_types ft ON ft.name = s.file_type_name
        WHERE s.is_from_zip_file = 0
          AND NOT EXISTS (
              SELECT 1 FROM files f
              WHERE f.dataset_id = s.dataset_id AND f.name = s.name
          )
    """)

    # ── Pass 2: zip-child files — parent now exists from pass 1 ───────────
    conn.execute("""
        INSERT INTO files (
            dataset_id, name, file_type_id, size_in_bytes,
            md5, url, is_from_zip_file, parent_zip_file_id
        )
        SELECT
            s.dataset_id,
            s.name,
            ft.file_type_id,
            s.size_in_bytes,
            s.md5,
            s.url,
            s.is_from_zip_file,
            parent.file_id
        FROM _stage_files s
        JOIN file_types ft ON ft.name = s.file_type_name
        LEFT JOIN files parent
               ON parent.dataset_id = s.dataset_id
              AND parent.name       = s.parent_zip_file_name
        WHERE s.is_from_zip_file = 1
          AND NOT EXISTS (
              SELECT 1 FROM files f
              WHERE f.dataset_id = s.dataset_id AND f.name = s.name
          )
    """)

    conn.commit()
    conn.unregister("_stage_files")

    n = conn.execute(
        """
        SELECT COUNT(*) FROM files
        WHERE dataset_id IN (SELECT unnest($1::INTEGER[]))
    """,
        [dataset_ids],
    ).fetchone()[0]
    logger.success(f"Files ingested — {n:,} total rows for these datasets.")


# ============================================================================
# Pipeline — simulation files (topology / parameter / trajectory)
#
# All three: register DataFrame view → one INSERT INTO ... SELECT joining
# against datasets and files. Zero Python loops, one SQL statement each.
# ============================================================================


def ingest_topology_files(
    df: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
    logger: "loguru.Logger" = loguru.logger,
) -> None:
    """Ingest topology files from the given DataFrame into the database."""
    conn.register("_stage_topo", df)
    conn.execute("""
        INSERT OR IGNORE INTO topology_files (
            file_id, atom_number, has_protein, has_nucleic,
            has_lipid, has_glucid, has_water_ion
        )
        SELECT f.file_id, s.atom_number, s.has_protein, s.has_nucleic,
               s.has_lipid, s.has_glucid, s.has_water_ion
        FROM _stage_topo s
        JOIN data_sources src ON src.name              = s.data_source
        JOIN datasets     d   ON d.data_source_id      = src.data_source_id
                             AND d.id_in_data_source   = s.dataset_id_in_data_source
        JOIN files        f   ON f.dataset_id = d.dataset_id AND f.name = s.name
    """)
    conn.commit()
    conn.unregister("_stage_topo")
    n = conn.execute("SELECT COUNT(*) FROM topology_files").fetchone()[0]
    logger.success(f"Topology files — {n:,} total rows in DB.")


def ingest_parameter_files(
    df: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
    logger: "loguru.Logger" = loguru.logger,
) -> None:
    """Ingest parameter files from the given DataFrame into the database."""
    conn.register("_stage_param", df)
    conn.execute("""
        INSERT OR IGNORE INTO parameter_files (
            file_id, dt, nsteps, temperature, thermostat, barostat, integrator
        )
        SELECT f.file_id, s.dt, s.nsteps, s.temperature,
               s.thermostat, s.barostat, s.integrator
        FROM _stage_param s
        JOIN data_sources src ON src.name              = s.data_source
        JOIN datasets     d   ON d.data_source_id      = src.data_source_id
                             AND d.id_in_data_source   = s.dataset_id_in_data_source
        JOIN files        f   ON f.dataset_id = d.dataset_id AND f.name = s.name
    """)
    conn.commit()
    conn.unregister("_stage_param")
    n = conn.execute("SELECT COUNT(*) FROM parameter_files").fetchone()[0]
    logger.success(f"Parameter files — {n:,} total rows in DB.")


def ingest_trajectory_files(
    df: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
    logger: "loguru.Logger" = loguru.logger,
) -> None:
    """Ingest trajectory files from the given DataFrame into the database."""
    conn.register("_stage_traj", df)
    conn.execute("""
        INSERT OR IGNORE INTO trajectory_files (file_id, atom_number, frame_number)
        SELECT f.file_id, s.atom_number, s.frame_number
        FROM _stage_traj s
        JOIN data_sources src ON src.name              = s.data_source
        JOIN datasets     d   ON d.data_source_id      = src.data_source_id
                             AND d.id_in_data_source   = s.dataset_id_in_data_source
        JOIN files        f   ON f.dataset_id = d.dataset_id AND f.name = s.name
    """)
    conn.commit()
    conn.unregister("_stage_traj")
    n = conn.execute("SELECT COUNT(*) FROM trajectory_files").fetchone()[0]
    logger.success(f"Trajectory files — {n:,} total rows in DB.")


# ============================================================================
# Entry point
# ============================================================================


def ingest(
    db_path: Path,
    data_type: str,
    parquet_path: Path,
    logger: "loguru.Logger" = loguru.logger,
) -> None:
    """Ingest data into the MDverse database."""
    db_conn = get_db_connection(db_path)

    logger.info(f"Data type  : {data_type}")
    logger.info(f"Source file: {parquet_path}")
    logger.info(f"Database   : {db_path.resolve()}")

    if data_type == "datasets":
        ingest_datasets(load_datasets_df(str(parquet_path)), db_conn)

    elif data_type == "files":
        df = load_files_df(str(parquet_path))

        source_names = df["data_source"].unique().tolist()
        all_dataset_ids = [
            r[0]
            for r in db_conn.execute(
                """
                SELECT d.dataset_id
                FROM datasets d
                JOIN data_sources src ON src.data_source_id = d.data_source_id
                WHERE src.name IN (SELECT unnest($1::VARCHAR[]))
            """,
                [source_names],
            ).fetchall()
        ]

        ids_with_files = (
            {
                r[0]
                for r in db_conn.execute(
                    """
                SELECT DISTINCT dataset_id FROM files
                WHERE dataset_id IN (SELECT unnest($1::INTEGER[]))
            """,
                    [all_dataset_ids],
                ).fetchall()
            }
            if all_dataset_ids
            else set()
        )

        new_dataset_ids = [d for d in all_dataset_ids if d not in ids_with_files]

        if not new_dataset_ids:
            logger.info("All datasets already have files — nothing to ingest.")
        else:
            logger.info(f"{len(new_dataset_ids):,} dataset(s) need file ingestion.")
            _delete_files_for_datasets(db_conn, new_dataset_ids)
            ingest_files(df, db_conn, dataset_ids=new_dataset_ids)

    elif data_type == "topology":
        ingest_topology_files(load_topology_df(str(parquet_path)), db_conn)

    elif data_type == "parameter":
        ingest_parameter_files(load_parameter_df(str(parquet_path)), db_conn)

    elif data_type == "trajectory":
        ingest_trajectory_files(load_trajectory_df(str(parquet_path)), db_conn)

    db_conn.close()


@click.command(
    help="Ingest data into the MDverse database.",
)
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True, file_okay=True, path_type=Path),
    required=True,
    help="Path to the DuckDB database file.",
)
@click.option(
    "--parquet",
    "parquet_path",
    type=click.Path(exists=True, file_okay=True, path_type=Path),
    required=True,
    help="Path to the parquet file.",
)
@click.option(
    "--type",
    "data_type",
    type=click.Choice(
        ["datasets", "files", "topology", "parameter", "trajectory"],
        case_sensitive=False,
    ),
    required=True,
    help="Define data type to be ingested.",
)
def main(db_path: Path, parquet_path: Path, data_type: str) -> None:
    """Ingest data into the MDverse database."""
    logpath = Path("logs/ingest_data_into_database.log")
    logger = create_logger(logpath=logpath, level="INFO")
    start_time = time.perf_counter()
    ingest(db_path, data_type, parquet_path, logger=logger)
    elapsed_time = str(timedelta(seconds=time.perf_counter() - start_time)).split(".")[
        0
    ]
    logger.info(f"Total time: {elapsed_time}")
    logger.success("Done.")


if __name__ == "__main__":
    main()
