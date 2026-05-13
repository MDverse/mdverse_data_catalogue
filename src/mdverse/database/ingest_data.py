"""
ingest_data.py
----------------------
Ingest parquet files into the MDverse SQLite database (database.db).

Prerequisites (run once before ingesting):
    python create_database.py --db database.db --schema database_schema.sql

Usage:
    uv run ingest_data.py /mdverse_sandbox/data/zenodo/2026-02-16/zenodo_datasets.parquet
    uv run ingest_data.py /mdverse_sandbox/data/zenodo/2026-02-16/zenodo_files.parquet
"""

import sys
import sqlite3
import argparse
import time
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger
from tqdm import tqdm

# ============================================================================
# Configuration
# ============================================================================

DB_PATH    = Path(__file__).parent / "database.db"
BATCH_SIZE = 5_000   # rows per commit; ~5 MB worst-case, safe for any SSD

SOURCE_URLS: dict[str, str] = {
    "zenodo":              "https://zenodo.org/",
    "figshare":            "https://figshare.com/",
    "atlas":               "https://www.dsimb.inserm.fr/ATLAS/",
    "nomad":               "https://nomad-lab.eu/",
    "gpcrmd":              "https://www.gpcrmd.org/",
    "mdposit_mmb_node":    "https://mmb.mddbr.eu/",
    "mdposit_inria_node":  "https://dynarepo.inria.fr/",
    "mdposit_cineca_node": "https://cineca.mddbr.eu/",
}

# ============================================================================
# Logging
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

# ============================================================================
# Database connection
# ============================================================================

def get_connection(db_path: Path) -> sqlite3.Connection:
    """Open the SQLite database with performance PRAGMAs enabled."""
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        logger.error("Run create_database.py first.")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys       = ON")
    conn.execute("PRAGMA journal_mode       = WAL")
    conn.execute("PRAGMA synchronous        = NORMAL")
    conn.execute("PRAGMA cache_size         = -64000")   # 64 MB page cache
    conn.execute("PRAGMA mmap_size          = 268435456") # 256 MB memory-map
    conn.execute("PRAGMA temp_store         = MEMORY")
    conn.execute("PRAGMA wal_autocheckpoint = 10000")
    return conn

# ============================================================================
# In-memory cache loaders  (one bulk SELECT each, called once per run)
# ============================================================================

def load_data_source_cache(conn: sqlite3.Connection) -> dict[str, int]:
    """name → data_source_id"""
    return dict(conn.execute("SELECT name, data_source_id FROM data_sources").fetchall())


def load_author_cache(conn: sqlite3.Connection) -> dict[str, int]:
    """name → author_id"""
    return dict(conn.execute("SELECT name, author_id FROM authors").fetchall())


def load_file_type_cache(conn: sqlite3.Connection) -> dict[str, int]:
    """name → file_type_id"""
    return dict(conn.execute("SELECT name, file_type_id FROM file_types").fetchall())


def load_dataset_cache(conn: sqlite3.Connection) -> dict[tuple[int, str], tuple]:
    """(data_source_id, id_in_data_source) → (dataset_id, *tracked_fields)"""
    rows = conn.execute(
        """
        SELECT dataset_id, data_source_id, id_in_data_source,
               doi, date_created, date_last_modified, date_last_crawled,
               file_number, url_in_data_source, title, description, keywords
        FROM datasets
        """
    ).fetchall()
    return {(r[1], r[2]): (r[0], *r[3:]) for r in rows}


def load_dataset_authors_cache(conn: sqlite3.Connection) -> dict[int, set[int]]:
    """dataset_id → set of author_ids"""
    cache: dict[int, set[int]] = {}
    for dataset_id, author_id in conn.execute(
        "SELECT dataset_id, author_id FROM datasets_authors_link"
    ).fetchall():
        cache.setdefault(dataset_id, set()).add(author_id)
    return cache


def load_dataset_id_cache(conn: sqlite3.Connection) -> dict[tuple[str, str], int]:
    """(data_source_name, id_in_data_source) → dataset_id"""
    return {
        (r[0], r[1]): r[2]
        for r in conn.execute(
            """
            SELECT ds.name, d.id_in_data_source, d.dataset_id
            FROM datasets d
            JOIN data_sources ds ON ds.data_source_id = d.data_source_id
            """
        ).fetchall()
    }


def load_file_existence_cache(
    conn: sqlite3.Connection,
    dataset_ids: list[int],
) -> set[tuple[int, str]]:
    """(dataset_id, file_name) set — O(1) duplicate check during file insert."""
    if not dataset_ids:
        return set()
    ph = ",".join("?" * len(dataset_ids))
    return set(
        conn.execute(
            f"SELECT dataset_id, name FROM files WHERE dataset_id IN ({ph})",
            dataset_ids,
        ).fetchall()
    )


def load_file_id_cache_for_type(
    conn: sqlite3.Connection,
    dataset_ids: list[int],
    file_type_name: str,
) -> dict[tuple[int, str], int]:
    """(dataset_id, file_name) → file_id, filtered to one file type."""
    if not dataset_ids:
        return {}
    ph = ",".join("?" * len(dataset_ids))
    rows = conn.execute(
        f"""
        SELECT f.dataset_id, f.name, f.file_id
        FROM files f
        JOIN file_types ft ON ft.file_type_id = f.file_type_id
        WHERE ft.name = ? AND f.dataset_id IN ({ph})
        """,
        [file_type_name, *dataset_ids],
    ).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}

# ============================================================================
# Cached upsert helper
# ============================================================================

def get_or_create_cached(
    conn: sqlite3.Connection,
    cache: dict[str, int],
    table: str,
    pk_col: str,
    lookup_col: str,
    lookup_val: str,
    extra_cols: dict | None = None,
) -> int:
    """
    Return the PK for lookup_val from cache.
    On cache miss: INSERT the row immediately (for lastrowid), update the
    cache, and return the new PK.  No commit — the row rides with the next
    batch commit.
    """
    if (pk := cache.get(lookup_val)) is not None:
        return pk

    cols = [lookup_col, *(extra_cols or {})]
    vals = [lookup_val, *(extra_cols or {}).values()]
    ph   = ", ".join("?" * len(vals))
    pk   = conn.execute(
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({ph})", vals
    ).lastrowid
    cache[lookup_val] = pk
    return pk

# ============================================================================
# DataFrame pre-processors
# All per-row work that can be vectorised is done here at C speed.
# ============================================================================

def load_datasets_df(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path, columns=[
        "dataset_repository_name", "dataset_id_in_repository",
        "doi", "date_created", "date_last_updated", "date_last_fetched",
        "number_of_files", "download_number", "view_number", "license",
        "dataset_url_in_repository", "title", "author_names",
        "keywords", "description",
    ])
    df = df.rename(columns={
        "dataset_repository_name":  "data_source",
        "dataset_id_in_repository": "id_in_data_source",
        "date_last_updated":        "date_last_modified",
        "date_last_fetched":        "date_last_crawled",
        "number_of_files":          "file_number",
        "dataset_url_in_repository":"url_in_data_source",
        "author_names":             "author",
    })

    # author_names is a Python list in the parquet (e.g. ["Smith, John", "Dong, Wei"]).
    # Join into a comma-separated string here — itertuples() silently converts
    # list columns to their string repr "['Smith, John']", destroying the structure.
    # Splitting back into names happens in the ingestion loop instead.
    df["author"] = df["author"].apply(
        lambda x: ",".join(x) if isinstance(x, (list, tuple, np.ndarray)) else ""
    )

    # Keywords: normalise separators, lowercase
    df["keywords"] = (
        df["keywords"].fillna("").astype(str)
        .str.replace(", ", ",", regex=False)
        .str.replace("; ", ";", regex=False)
        .str.replace(",", ";", regex=False)
        .str.lower()
    )

    # Integer columns: coerce NaN → 0
    for col in ("file_number", "download_number", "view_number"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Nullable string columns: NaN → None for sqlite3 binding
    for col in ("doi", "license", "description", "keywords", "url_in_data_source", "title"):
        df[col] = df[col].where(df[col].notna(), other=None)

    df["data_source_url"] = df["data_source"].map(SOURCE_URLS)
    return df


def load_files_df(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path, columns=[
        "dataset_repository_name", "dataset_id_in_repository",
        "file_name", "file_url_in_repository", "file_size_in_bytes",
        "file_md5", "containing_archive_file_name", "file_type",
    ])
    df = df.rename(columns={
        "dataset_repository_name":      "data_source",
        "dataset_id_in_repository":     "dataset_id_in_data_source",
        "file_name":                    "name",
        "file_url_in_repository":       "url",
        "file_size_in_bytes":           "size_in_bytes",
        "file_md5":                     "md5",
        "containing_archive_file_name": "parent_zip_file_name",
        "file_type":                    "type",
    })
    df["is_from_zip_file"] = df["parent_zip_file_name"].notna().astype(int)
    for col in ("size_in_bytes", "md5", "url", "parent_zip_file_name"):
        df[col] = df[col].where(df[col].notna(), other=None)
    return df


def load_topology_df(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path, columns=[
        "dataset_origin", "dataset_id", "file_name",
        "atom_number", "has_protein", "has_nucleic",
        "has_lipid", "has_glucid", "has_water_ion",
    ])
    df = df.rename(columns={
        "dataset_origin": "data_source",
        "dataset_id":     "dataset_id_in_data_source",
        "file_name":      "name",
    })
    for col in ("has_protein", "has_nucleic", "has_lipid", "has_glucid", "has_water_ion"):
        df[col] = df[col].astype(int)
    df["dataset_id_in_data_source"] = df["dataset_id_in_data_source"].astype(str)
    return df


def load_parameter_df(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path, columns=[
        "dataset_origin", "dataset_id", "file_name",
        "dt", "nsteps", "temperature", "thermostat", "barostat", "integrator",
    ])
    df = df.rename(columns={
        "dataset_origin": "data_source",
        "dataset_id":     "dataset_id_in_data_source",
        "file_name":      "name",
    })
    df["integrator"] = df["integrator"].fillna("undefined")
    for col in ("dt", "nsteps", "temperature", "thermostat", "barostat"):
        df[col] = df[col].where(df[col].notna(), other=None)
    df["dataset_id_in_data_source"] = df["dataset_id_in_data_source"].astype(str)
    return df


def load_trajectory_df(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path, columns=[
        "dataset_origin", "dataset_id", "file_name",
        "atom_number", "frame_number",
    ])
    df = df.rename(columns={
        "dataset_origin": "data_source",
        "dataset_id":     "dataset_id_in_data_source",
        "file_name":      "name",
    })
    df["dataset_id_in_data_source"] = df["dataset_id_in_data_source"].astype(str)
    return df

# ============================================================================
# Ingestion — datasets
# ============================================================================

def ingest_datasets(df: pd.DataFrame, conn: sqlite3.Connection) -> list[int]:
    """
    Insert or update datasets, authors, data_sources, and datasets_authors_link.
    Returns dataset_ids of all rows that were created or modified.
    """
    source_cache  = load_data_source_cache(conn)
    author_cache  = load_author_cache(conn)
    dataset_cache = load_dataset_cache(conn)
    da_cache      = load_dataset_authors_cache(conn)

    new_ids, modified_ids, unchanged_ids = [], [], []
    pending       = 0
    pending_links: list[tuple[int, int]] = []

    for row in tqdm(df.itertuples(index=False), total=len(df), desc="Datasets", unit="row"):

        source_id = get_or_create_cached(
            conn, source_cache, "data_sources", "data_source_id", "name",
            row.data_source,
            extra_cols={"url": row.data_source_url, "citation": None, "comment": None},
        )

        author_ids = [
            get_or_create_cached(
                conn, author_cache, "authors", "author_id", "name", name,
                extra_cols={"orcid": None},
            )
            for name in [n.strip() for n in row.author.split(",") if n.strip()]
        ]

        cache_key = (source_id, row.id_in_data_source)
        existing  = dataset_cache.get(cache_key)

        if not existing:
            dataset_id = conn.execute(
                """
                INSERT INTO datasets (
                    data_source_id, id_in_data_source, url_in_data_source,
                    doi, date_created, date_last_modified, date_last_crawled,
                    file_number, download_number, view_number,
                    license, title, description, keywords
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    source_id, row.id_in_data_source, row.url_in_data_source,
                    row.doi, row.date_created, row.date_last_modified,
                    row.date_last_crawled, row.file_number,
                    row.download_number, row.view_number,
                    row.license, row.title, row.description, row.keywords,
                ),
            ).lastrowid

            pending_links.extend((dataset_id, aid) for aid in author_ids)
            dataset_cache[cache_key] = (
                dataset_id, row.doi, row.date_created, row.date_last_modified,
                row.date_last_crawled, row.file_number, row.url_in_data_source,
                row.title, row.description, row.keywords,
            )
            da_cache[dataset_id] = set(author_ids)
            new_ids.append(dataset_id)

        else:
            (dataset_id, db_doi, db_date_created, db_date_last_modified,
             db_date_last_crawled, db_file_number, db_url,
             db_title, db_description, db_keywords) = existing

            fields_changed = (
                db_doi                   != row.doi
                or db_date_created       != row.date_created
                or db_date_last_modified != row.date_last_modified
                or db_date_last_crawled  != row.date_last_crawled
                or db_file_number        != row.file_number
                or db_url                != row.url_in_data_source
                or db_title              != row.title
                or db_description        != row.description
                or db_keywords           != row.keywords
            )
            authors_changed = set(author_ids) != da_cache.get(dataset_id, set())

            if fields_changed:
                conn.execute(
                    """
                    UPDATE datasets SET
                        doi = ?, date_created = ?, date_last_modified = ?,
                        date_last_crawled = ?, file_number = ?,
                        url_in_data_source = ?, title = ?,
                        description = ?, keywords = ?
                    WHERE dataset_id = ?
                    """,
                    (
                        row.doi, row.date_created, row.date_last_modified,
                        row.date_last_crawled, row.file_number,
                        row.url_in_data_source, row.title,
                        row.description, row.keywords, dataset_id,
                    ),
                )
                dataset_cache[cache_key] = (
                    dataset_id, row.doi, row.date_created, row.date_last_modified,
                    row.date_last_crawled, row.file_number, row.url_in_data_source,
                    row.title, row.description, row.keywords,
                )

            if authors_changed:
                conn.execute(
                    "DELETE FROM datasets_authors_link WHERE dataset_id = ?",
                    (dataset_id,),
                )
                pending_links.extend((dataset_id, aid) for aid in author_ids)
                da_cache[dataset_id] = set(author_ids)

            if fields_changed or authors_changed:
                modified_ids.append(dataset_id)
            else:
                unchanged_ids.append(dataset_id)

        pending += 1
        if pending >= BATCH_SIZE:
            if pending_links:
                conn.executemany(
                    "INSERT OR IGNORE INTO datasets_authors_link "
                    "(dataset_id, author_id) VALUES (?, ?)",
                    pending_links,
                )
                pending_links = []
            conn.commit()
            pending = 0

    # Final flush
    if pending_links:
        conn.executemany(
            "INSERT OR IGNORE INTO datasets_authors_link "
            "(dataset_id, author_id) VALUES (?, ?)",
            pending_links,
        )
    if pending:
        conn.commit()

    logger.success("Completed dataset ingestion.")
    logger.info(f"Created: {len(new_ids)}  |  Updated: {len(modified_ids)}  |  Unchanged: {len(unchanged_ids)}")
    return new_ids + modified_ids

# ============================================================================
# Ingestion — files
# ============================================================================

def _delete_files_for_datasets(conn: sqlite3.Connection, dataset_ids: list[int]) -> None:
    """Delete all files (and their simulation children) for the given datasets."""
    if not dataset_ids:
        return

    CHUNK = 999  # SQLite hard limit on variables per query
    file_ids = []
    for i in range(0, len(dataset_ids), CHUNK):
        chunk = dataset_ids[i : i + CHUNK]
        ph    = ",".join("?" * len(chunk))
        file_ids.extend(
            r[0] for r in conn.execute(
                f"SELECT file_id FROM files WHERE dataset_id IN ({ph})", chunk
            ).fetchall()
        )

    if file_ids:
        for i in range(0, len(file_ids), CHUNK):
            chunk = file_ids[i : i + CHUNK]
            ph    = ",".join("?" * len(chunk))
            for table in ("topology_files", "parameter_files", "trajectory_files"):
                conn.execute(f"DELETE FROM {table} WHERE file_id IN ({ph})", chunk)

    for i in range(0, len(dataset_ids), CHUNK):
        chunk = dataset_ids[i : i + CHUNK]
        ph    = ",".join("?" * len(chunk))
        conn.execute(f"DELETE FROM files WHERE dataset_id IN ({ph})", chunk)

    conn.commit()
    logger.info(f"Deleted existing files for {len(dataset_ids)} dataset(s).")


def ingest_files(
    df: pd.DataFrame,
    conn: sqlite3.Connection,
    dataset_ids: list[int],
) -> None:
    """Insert file rows for the given dataset_ids."""
    if not dataset_ids:
        logger.info("No datasets to process — skipping file ingestion.")
        return

    eligible         = set(dataset_ids)
    dataset_id_cache = load_dataset_id_cache(conn)
    file_type_cache  = load_file_type_cache(conn)
    existing_files   = load_file_existence_cache(conn, dataset_ids)

    created = skipped = pending = 0
    parent_cache: dict[tuple[int, str], int] = {}

    for row in tqdm(df.itertuples(index=False), total=len(df), desc="Files", unit="row"):

        dataset_id = dataset_id_cache.get((row.data_source, row.dataset_id_in_data_source))
        if dataset_id is None or dataset_id not in eligible:
            skipped += 1
            continue

        if (dataset_id, row.name) in existing_files:
            skipped += 1
            continue

        file_type_id = get_or_create_cached(
            conn, file_type_cache, "file_types", "file_type_id", "name", row.type,
            extra_cols={"comment": None},
        )

        parent_zip_file_id = None
        if row.is_from_zip_file and row.parent_zip_file_name:
            parent_zip_file_id = parent_cache.get((dataset_id, row.parent_zip_file_name))
            if parent_zip_file_id is None:
                logger.debug(
                    f"Parent zip '{row.parent_zip_file_name}' not found "
                    f"for '{row.name}' in dataset {dataset_id}."
                )

        file_id = conn.execute(
            """
            INSERT INTO files (
                dataset_id, name, file_type_id, size_in_bytes,
                md5, url, is_from_zip_file, parent_zip_file_id
            ) VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                dataset_id, row.name, file_type_id, row.size_in_bytes,
                row.md5, row.url, row.is_from_zip_file, parent_zip_file_id,
            ),
        ).lastrowid

        existing_files.add((dataset_id, row.name))
        created += 1

        if not row.is_from_zip_file and row.type == "zip":
            parent_cache[(dataset_id, row.name)] = file_id

        pending += 1
        if pending >= BATCH_SIZE:
            conn.commit()
            pending = 0

    if pending:
        conn.commit()

    logger.success("Completed file ingestion.")
    logger.info(f"Created: {created}  |  Skipped: {skipped}")

# ============================================================================
# Ingestion — simulation files (topology / parameter / trajectory)
#
# Shared pattern for all three:
#   1. Load dataset_id_cache and a type-filtered file_id_cache once.
#   2. Single itertuples pass — zero SQL in the loop, pure dict lookups.
#   3. One executemany() + one commit for the whole table.
# ============================================================================

def _referenced_dataset_ids(
    df: pd.DataFrame,
    dataset_id_cache: dict[tuple[str, str], int],
) -> list[int]:
    """Collect DB dataset_ids referenced in df in a single pass."""
    return list({
        did
        for row in df.itertuples(index=False)
        if (did := dataset_id_cache.get(
            (row.data_source, row.dataset_id_in_data_source)
        )) is not None
    })


def ingest_topology_files(df: pd.DataFrame, conn: sqlite3.Connection) -> None:
    dataset_id_cache = load_dataset_id_cache(conn)
    ref_ids          = _referenced_dataset_ids(df, dataset_id_cache)
    file_id_cache    = load_file_id_cache_for_type(conn, ref_ids, "gro")

    rows, missing = [], 0
    for row in tqdm(df.itertuples(index=False), total=len(df), desc="Topology files", unit="row"):
        dataset_id = dataset_id_cache.get((row.data_source, row.dataset_id_in_data_source))
        file_id    = file_id_cache.get((dataset_id, row.name)) if dataset_id else None
        if file_id is None:
            missing += 1
            continue
        rows.append((
            file_id, row.atom_number,
            row.has_protein, row.has_nucleic, row.has_lipid,
            row.has_glucid, row.has_water_ion,
        ))

    if rows:
        conn.executemany(
            """
            INSERT OR IGNORE INTO topology_files (
                file_id, atom_number, has_protein, has_nucleic,
                has_lipid, has_glucid, has_water_ion
            ) VALUES (?,?,?,?,?,?,?)
            """,
            rows,
        )
        conn.commit()

    logger.success(f"Topology files — inserted: {len(rows)}  |  skipped: {missing}")


def ingest_parameter_files(df: pd.DataFrame, conn: sqlite3.Connection) -> None:
    dataset_id_cache = load_dataset_id_cache(conn)
    ref_ids          = _referenced_dataset_ids(df, dataset_id_cache)
    file_id_cache    = load_file_id_cache_for_type(conn, ref_ids, "mdp")

    rows, missing = [], 0
    for row in tqdm(df.itertuples(index=False), total=len(df), desc="Parameter files", unit="row"):
        dataset_id = dataset_id_cache.get((row.data_source, row.dataset_id_in_data_source))
        file_id    = file_id_cache.get((dataset_id, row.name)) if dataset_id else None
        if file_id is None:
            missing += 1
            continue
        rows.append((
            file_id, row.dt, row.nsteps, row.temperature,
            row.thermostat, row.barostat, row.integrator,
        ))

    if rows:
        conn.executemany(
            """
            INSERT OR IGNORE INTO parameter_files (
                file_id, dt, nsteps, temperature, thermostat, barostat, integrator
            ) VALUES (?,?,?,?,?,?,?)
            """,
            rows,
        )
        conn.commit()

    logger.success(f"Parameter files — inserted: {len(rows)}  |  skipped: {missing}")


def ingest_trajectory_files(df: pd.DataFrame, conn: sqlite3.Connection) -> None:
    dataset_id_cache = load_dataset_id_cache(conn)
    ref_ids          = _referenced_dataset_ids(df, dataset_id_cache)
    file_id_cache    = load_file_id_cache_for_type(conn, ref_ids, "xtc")

    rows, missing = [], 0
    for row in tqdm(df.itertuples(index=False), total=len(df), desc="Trajectory files", unit="row"):
        dataset_id = dataset_id_cache.get((row.data_source, row.dataset_id_in_data_source))
        file_id    = file_id_cache.get((dataset_id, row.name)) if dataset_id else None
        if file_id is None:
            missing += 1
            continue
        rows.append((file_id, row.atom_number, row.frame_number))

    if rows:
        conn.executemany(
            "INSERT OR IGNORE INTO trajectory_files "
            "(file_id, atom_number, frame_number) VALUES (?,?,?)",
            rows,
        )
        conn.commit()

    logger.success(f"Trajectory files — inserted: {len(rows)}  |  skipped: {missing}")

# ============================================================================
# Parquet type auto-detection
# ============================================================================

def detect_parquet_type(path: Path) -> str:
    name = path.name.lower()
    if "topology"             in name: return "topology"
    if "parameter" in name or "mdp" in name: return "parameter"
    if "trajectory" in name or "xtc" in name: return "trajectory"
    if "dataset"              in name: return "datasets"
    if "file"                 in name: return "files"
    raise ValueError(
        f"Cannot detect parquet type from '{path.name}'. "
        "Expected name to contain: dataset, file, topology, parameter, or trajectory."
    )

# ============================================================================
# Entry point
# ============================================================================

def ingest(parquet_path: Path, db_path: Path) -> None:
    conn = get_connection(db_path)
    kind = detect_parquet_type(parquet_path)
    logger.info(f"Parquet type : {kind}")
    logger.info(f"Source file  : {parquet_path}")
    logger.info(f"Database     : {db_path.resolve()}")

    if kind == "datasets":
        ingest_datasets(load_datasets_df(str(parquet_path)), conn)

    elif kind == "files":
        df               = load_files_df(str(parquet_path))
        dataset_id_cache = load_dataset_id_cache(conn)

        all_dataset_ids = list({
            did
            for row in df.itertuples(index=False)
            if (did := dataset_id_cache.get(
                (row.data_source, row.dataset_id_in_data_source)
            )) is not None
        })

        # Only re-ingest datasets that have no files yet.
        # Datasets that already have files are considered up to date — skip them.
        if all_dataset_ids:
            CHUNK = 999
            ids_with_files = set()
            for i in range(0, len(all_dataset_ids), CHUNK):
                chunk = all_dataset_ids[i : i + CHUNK]
                ph    = ",".join("?" * len(chunk))
                rows  = conn.execute(
                    f"SELECT DISTINCT dataset_id FROM files WHERE dataset_id IN ({ph})",
                    chunk,
                ).fetchall()
                ids_with_files.update(r[0] for r in rows)

        new_dataset_ids = [d for d in all_dataset_ids if d not in ids_with_files]

        if not new_dataset_ids:
            logger.info("All datasets already have files — nothing to ingest.")
        else:
            logger.info(f"{len(new_dataset_ids)} dataset(s) need file ingestion.")
            _delete_files_for_datasets(conn, new_dataset_ids)
            ingest_files(df, conn, dataset_ids=new_dataset_ids)

    elif kind == "topology":
        ingest_topology_files(load_topology_df(str(parquet_path)), conn)

    elif kind == "parameter":
        ingest_parameter_files(load_parameter_df(str(parquet_path)), conn)

    elif kind == "trajectory":
        ingest_trajectory_files(load_trajectory_df(str(parquet_path)), conn)

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest a parquet file into the MDverse SQLite database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    uv run ingest_data.py /mdverse_sandbox/data/zenodo/2026-02-16/zenodo_datasets.parquet
    uv run ingest_data.py /mdverse_sandbox/data/zenodo/2026-02-16/zenodo_files.parquet
        """,
    )
    parser.add_argument("parquet", metavar="PARQUET_FILE", help="Path to the parquet file.")
    parser.add_argument(
        "--db",
        metavar="PATH",
        default=str(DB_PATH),
        help=f"Path to the SQLite database (default: {DB_PATH}).",
    )
    args = parser.parse_args()

    parquet_path = Path(args.parquet)
    db_path      = Path(args.db)

    if not parquet_path.exists():
        logger.error(f"Parquet file not found: {parquet_path}")
        sys.exit(1)

    start = time.perf_counter()
    ingest(parquet_path, db_path)
    elapsed = str(timedelta(seconds=time.perf_counter() - start)).split(".")[0]
    logger.info(f"Total time: {elapsed}")
    logger.success("Done.")


if __name__ == "__main__":
    main()
