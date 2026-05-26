"""
delete_data.py
---------------------
Remove data from the MDverse DuckDB database in two modes:

  DATASET mode — removes a single dataset identified by its repository
                 name (--datarepo) and its source ID (i.e id_in_data_source) within that repository
                 (--dataset).

  SOURCE mode  — removes ALL datasets and every related record belonging
                 to a given data source; omit --dataset to trigger this mode.

Deletion order (child tables first, parent tables last):

    MoleculeExternalDB ─┐
    Molecule            ├─► Annotation ─┐
                                        │
    TopologyFile    ─┐                  │
    ParameterFile   ├─► File ───────────┼─► Dataset ──► DataSource
    TrajectoryFile  ─┘                  │
    DatasetAuthorLink ──────────────────┘

    The files table has a self-referencing FK (parent_zip_file_id), so it
    requires two passes: zip-children deleted before zip-parents.

Note on transactions:
    DuckDB v1.x enforces foreign key (FK) constraints per-statement, even
    inside a BEGIN/COMMIT block. This means a single transaction cannot delete
    rows from multiple tables that reference each other via FK constraints —
    each DELETE would fail because the referenced rows in the parent table still
    exist at the time of execution.

    The standard workaround is to use autocommit with a strict child-first
    deletion order: we always delete from the most deeply nested child table
    first, working our way up to the parent. This guarantees no FK violations
    and no orphaned rows at any point. If a failure occurs mid-way, the database
    remains consistent and the operation can be safely retried.

Usage:
    uv run delete_data.py --datarepo zenodo --dry-run
    uv run delete_data.py --datarepo zenodo
    uv run delete_data.py --datarepo zenodo --dataset <id_in_data_source> --dry-run
    uv run delete_data.py --datarepo zenodo --dataset <id_in_data_source>
"""


import sys
import argparse
import time
from datetime import timedelta
from pathlib import Path

import duckdb
from loguru import logger


# ── Configuration ──────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "database.duckdb"


# ── Connection ─────────────────────────────────────────────────────────────────

def get_connection() -> duckdb.DuckDBPyConnection:
    if not DB_PATH.exists():
        logger.error(f"Database not found: {DB_PATH}")
        sys.exit(1)
    return duckdb.connect(str(DB_PATH))


# ── Helpers ────────────────────────────────────────────────────────────────────

def fetch_one(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None):
    return conn.execute(sql, params or []).fetchone()


def fetch_ids(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> list[int]:
    """Execute a SELECT and return a flat list of IDs."""
    rows = conn.execute(sql, params or []).fetchall()
    return [row[0] for row in rows]


# ── ID collection ──────────────────────────────────────────────────────────────
#
#   we first collect all relevant IDs into Python lists. 
#   This makes the deletion logic simpler and easier to follow:
#   1. Collect dataset_ids  → used to find file_ids and annotation_ids
#   2. Collect file_ids     → used to delete simulation file metadata
#   3. Collect annotation_ids → used to find molecule_ids
#   4. Collect molecule_ids → used to delete molecules_external_db rows

def collect_ids_for_dataset(conn: duckdb.DuckDBPyConnection, dataset_id: int) -> dict[str, list[int]]:
    """Collect all related IDs for a single dataset."""
    dataset_ids    = [dataset_id]
    file_ids       = fetch_ids(conn, "SELECT file_id       FROM files       WHERE dataset_id    = ?", [dataset_id])
    annotation_ids = fetch_ids(conn, "SELECT annotation_id FROM annotations WHERE dataset_id    = ?", [dataset_id])
    molecule_ids   = fetch_ids(conn, "SELECT molecule_id   FROM molecules   WHERE annotation_id IN (SELECT annotation_id FROM annotations WHERE dataset_id = ?)", [dataset_id])
    return {
        "dataset_ids":    dataset_ids,
        "file_ids":       file_ids,
        "annotation_ids": annotation_ids,
        "molecule_ids":   molecule_ids,
    }


def collect_ids_for_source(conn: duckdb.DuckDBPyConnection, source_id: int) -> dict[str, list[int]]:
    """Collect all related IDs for all datasets belonging to a data source."""
    dataset_ids    = fetch_ids(conn, "SELECT dataset_id    FROM datasets    WHERE data_source_id = ?", [source_id])
    if not dataset_ids:
        return {"dataset_ids": [], "file_ids": [], "annotation_ids": [], "molecule_ids": []}
    placeholders   = ",".join("?" * len(dataset_ids))
    file_ids       = fetch_ids(conn, f"SELECT file_id       FROM files       WHERE dataset_id    IN ({placeholders})", dataset_ids)
    annotation_ids = fetch_ids(conn, f"SELECT annotation_id FROM annotations WHERE dataset_id    IN ({placeholders})", dataset_ids)
    molecule_ids   = fetch_ids(conn, f"SELECT molecule_id   FROM molecules   WHERE annotation_id IN ({','.join('?' * len(annotation_ids))})", annotation_ids) if annotation_ids else []
    return {
        "dataset_ids":    dataset_ids,
        "file_ids":       file_ids,
        "annotation_ids": annotation_ids,
        "molecule_ids":   molecule_ids,
    }


# ── Row count and delete helper ────────────────────────────────────────────────

def count_rows_by_ids(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    ids: list[int],
) -> int:
    """Count rows in a table where column matches any of the given IDs."""
    if not ids:
        return 0
    placeholders = ",".join("?" * len(ids))
    return conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IN ({placeholders})", ids).fetchone()[0]


def delete_rows_by_ids(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    ids: list[int],
) -> int:
    """Delete rows from a table where column matches any of the given IDs. Returns deleted count."""
    if not ids:
        return 0
    placeholders = ",".join("?" * len(ids))
    return conn.execute(f"DELETE FROM {table} WHERE {column} IN ({placeholders})", ids).fetchone()[0]


# ── Core deletion logic ────────────────────────────────────────────────────────

def _count(ids: dict[str, list[int]], conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Return row counts per table for dry-run preview. No data is modified."""
    return {
        "MoleculeExternalDB": count_rows_by_ids(conn, "molecules_external_db", "molecule_id",   ids["molecule_ids"]),
        "Molecule":           count_rows_by_ids(conn, "molecules",             "annotation_id", ids["annotation_ids"]),
        "Annotation":         count_rows_by_ids(conn, "annotations",           "dataset_id",    ids["dataset_ids"]),
        "TopologyFile":       count_rows_by_ids(conn, "topology_files",        "file_id",       ids["file_ids"]),
        "ParameterFile":      count_rows_by_ids(conn, "parameter_files",       "file_id",       ids["file_ids"]),
        "TrajectoryFile":     count_rows_by_ids(conn, "trajectory_files",      "file_id",       ids["file_ids"]),
        "DatasetAuthorLink":  count_rows_by_ids(conn, "datasets_authors_link", "dataset_id",    ids["dataset_ids"]),
        "File":               count_rows_by_ids(conn, "files",                 "dataset_id",    ids["dataset_ids"]),
        "Dataset":            count_rows_by_ids(conn, "datasets",              "dataset_id",    ids["dataset_ids"]),
    }


def _delete(ids: dict[str, list[int]], conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """
    Delete all related records in child-first order.
    Each statement runs in autocommit — see module docstring for rationale.
    """
    counts: dict[str, int] = {}

    # 1. molecules_external_db — deepest child in annotation chain
    counts["MoleculeExternalDB"] = delete_rows_by_ids(conn, "molecules_external_db", "molecule_id",   ids["molecule_ids"])

    # 2. molecules
    counts["Molecule"]           = delete_rows_by_ids(conn, "molecules",             "annotation_id", ids["annotation_ids"])

    # 3. annotations
    counts["Annotation"]         = delete_rows_by_ids(conn, "annotations",           "dataset_id",    ids["dataset_ids"])

    # 4. simulation-file metadata
    counts["TopologyFile"]       = delete_rows_by_ids(conn, "topology_files",        "file_id",       ids["file_ids"])
    counts["ParameterFile"]      = delete_rows_by_ids(conn, "parameter_files",       "file_id",       ids["file_ids"])
    counts["TrajectoryFile"]     = delete_rows_by_ids(conn, "trajectory_files",      "file_id",       ids["file_ids"])

    # 5. dataset–author links
    counts["DatasetAuthorLink"]  = delete_rows_by_ids(conn, "datasets_authors_link", "dataset_id",    ids["dataset_ids"])

    # 6. files — two passes for self-referencing parent_zip_file_id FK
    #    zip-children (files inside a zip) must be deleted before zip-parents
    if ids["file_ids"]:
        placeholders = ",".join("?" * len(ids["file_ids"]))
        total_files = 0
        for filter_clause in ("parent_zip_file_id IS NOT NULL", "parent_zip_file_id IS NULL"):
            total_files += conn.execute(
                f"SELECT COUNT(*) FROM files WHERE file_id IN ({placeholders}) AND {filter_clause}",
                ids["file_ids"]
            ).fetchone()[0]
            conn.execute(
                f"DELETE FROM files WHERE file_id IN ({placeholders}) AND {filter_clause}",
                ids["file_ids"]
            )
        counts["File"] = total_files
    else:
        counts["File"] = 0

    # 7. datasets
    counts["Dataset"]            = delete_rows_by_ids(conn, "datasets", "dataset_id", ids["dataset_ids"])

    return counts


def _log_counts(counts: dict[str, int], dry_run: bool) -> None:
    prefix = "[DRY-RUN] Would delete" if dry_run else "Deleted"
    for label, count in counts.items():
        logger.info(f"{prefix} {count:>7,} row(s) from {label}")


# ── Public entry points ────────────────────────────────────────────────────────

def delete_dataset(source_name: str, id_in_source: str, dry_run: bool = False) -> None:
    """Remove a single dataset and all its related records."""
    logger.info(f"Mode: DELETE DATASET  |  datarepo='{source_name}'  dataset='{id_in_source}'")
    if dry_run:
        logger.warning("DRY-RUN — no changes will be written.")

    conn = get_connection()

    source = fetch_one(conn, "SELECT data_source_id FROM data_sources WHERE name = ?", [source_name])
    if not source:
        logger.error(f"Data source '{source_name}' not found.")
        conn.close(); sys.exit(1)

    dataset = fetch_one(
        conn,
        "SELECT dataset_id, title FROM datasets WHERE data_source_id = ? AND id_in_data_source = ?",
        [source[0], id_in_source],
    )
    if not dataset:
        logger.error(f"Dataset '{id_in_source}' not found in '{source_name}'.")
        conn.close(); sys.exit(1)

    logger.info(f"Found dataset PK={dataset[0]}  title='{dataset[1]}'")

    ids = collect_ids_for_dataset(conn, dataset[0])

    if dry_run:
        counts = _count(ids, conn)
    else:
        try:
            counts = _delete(ids, conn)
            logger.success("Deletion complete.")
        except Exception as exc:
            logger.error(f"Deletion failed: {exc}")
            conn.close(); sys.exit(1)

    conn.close()
    _log_counts(counts, dry_run)


def delete_source(source_name: str, dry_run: bool = False) -> None:
    """Remove ALL datasets belonging to a data source, then the source itself."""
    logger.info(f"Mode: DELETE SOURCE  |  datarepo='{source_name}'")
    if dry_run:
        logger.warning("DRY-RUN — no changes will be written.")

    conn = get_connection()

    source = fetch_one(conn, "SELECT data_source_id FROM data_sources WHERE name = ?", [source_name])
    if not source:
        logger.error(f"Data source '{source_name}' not found.")
        conn.close(); sys.exit(1)

    dataset_count = fetch_one(
        conn,
        "SELECT COUNT(*) FROM datasets WHERE data_source_id = ?",
        [source[0]],
    )[0]
    logger.info(f"Found {dataset_count:,} dataset(s) under '{source_name}'.")

    ids = collect_ids_for_source(conn, source[0])

    if dry_run:
        counts = _count(ids, conn)
        conn.close()
        counts["DataSource"] = 1
    else:
        conn.close()
        answer = input(
            f"\n  WARNING: This will permanently delete ALL {dataset_count:,} datasets "
            f"and all related records for '{source_name}'.\n"
            f"  Type the source name to confirm: "
        ).strip()
        if answer != source_name:
            logger.warning("Confirmation did not match. Aborting.")
            sys.exit(0)

        conn = get_connection()
        try:
            counts = _delete(ids, conn)
            conn.execute("DELETE FROM data_sources WHERE name = ?", [source_name])
            counts["DataSource"] = 1
            logger.success("Deletion complete.")
        except Exception as exc:
            logger.error(f"Deletion failed: {exc}")
            logger.warning("Re-run with --dry-run to check remaining data.")
            conn.close(); sys.exit(1)

        conn.close()

    _log_counts(counts, dry_run)


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Remove datasets or entire data sources from the MDverse DuckDB database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  uv run delete_data.py --datarepo zenodo --dry-run\n"
            "  uv run delete_data.py --datarepo zenodo\n"
            "  uv run delete_data.py --datarepo zenodo --dataset <ID_IN_SOURCE> --dry-run\n"
            "  uv run delete_data.py --datarepo zenodo --dataset <ID_IN_SOURCE>"
        ),
    )
    parser.add_argument(
        "--datarepo",
        required=True,
        metavar="SOURCE_NAME",
        help="Repository name to target (e.g. zenodo, atlas, nomad).",
    )
    parser.add_argument(
        "--dataset",
        metavar="ID_IN_SOURCE",
        default=None,
        help="ID of a single dataset within --datarepo. "
             "Omit to delete ALL datasets for that repo.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be deleted without making any changes.",
    )

    args = parser.parse_args()
    start = time.perf_counter()

    if args.dataset:
        delete_dataset(args.datarepo, args.dataset, dry_run=args.dry_run)
    else:
        delete_source(args.datarepo, dry_run=args.dry_run)

    elapsed = str(timedelta(seconds=time.perf_counter() - start)).split(".")[0]
    logger.info(f"Total time: {elapsed}")
    logger.success("Done.")


if __name__ == "__main__":
    main()