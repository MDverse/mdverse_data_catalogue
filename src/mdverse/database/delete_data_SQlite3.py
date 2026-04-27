"""
Purpose:
    Same deletion functionality as delete_data.py but using Python's built-in
    sqlite3 module directly instead of SQLModel/SQLAlchemy.

    Two modes:
    1. DATASET mode — removes a single dataset by source name + ID in source.
    2. SOURCE mode  — removes ALL datasets belonging to a data source.

How it works:
    Plain SQL strings are executed directly against the database connection.
    Deletions cascade from deepest child to parent:

        TopologyFile  ─┐
        ParameterFile  ├─► File ──► Dataset ──► DataSource (source mode only)
        TrajectoryFile ─┘

    All deletions happen inside ONE transaction — if anything fails, the
    entire transaction is rolled back automatically.

Performance:
    IDs are collected with a single SELECT, then deleted table by table with
    bulk DELETE ... WHERE id IN (...) statements — chunked at 999 to respect
    SQLite's variable limit.

Why sqlite3 instead of SQLModel?
    sqlite3 is Python's built-in module — no dependencies, no installation.
    SQLModel/SQLAlchemy are wrappers around it that add convenience but also
    complexity. sqlite3 is simpler and more transparent.

Usage:
    # Delete one dataset
    python delete_data_sqlite3.py --dataset zenodo 1234567

    # Delete ALL datasets from a source
    python delete_data_sqlite3.py --source zenodo

    # Dry-run (safe preview — nothing is deleted)
    python delete_data_sqlite3.py --dataset zenodo 1234567 --dry-run
    python delete_data_sqlite3.py --source zenodo --dry-run
"""

import sys
import sqlite3
import argparse
import time
from datetime import timedelta
from pathlib import Path

# ============================================================================
# Configuration
# ============================================================================

DB_PATH = Path(__file__).parent / "database.db"  # Always resolves to the
                                                  # directory of this script
SQLITE_MAX_VARS = 999  # SQLite hard limit on parameters per query


# ============================================================================
# Helpers
# ============================================================================

def get_connection() -> sqlite3.Connection:
    """
    Open a connection to the database with foreign key enforcement enabled.

    By default SQLite does NOT enforce foreign keys — you must enable it
    explicitly per connection with PRAGMA foreign_keys = ON.
    """
    if not DB_PATH.exists():
        print(f"ERROR | Database not found: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")  # Enforce FK constraints
    conn.row_factory = sqlite3.Row           # Rows behave like dicts
    return conn


def chunked(ids: list[int], size: int = SQLITE_MAX_VARS):
    """Yield successive chunks of `size` from a list of IDs."""
    for i in range(0, len(ids), size):
        yield ids[i : i + size]


def fetch_one(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> sqlite3.Row | None:
    """Execute a SELECT and return the first row, or None."""
    return conn.execute(sql, params).fetchone()


def fetch_all(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    """Execute a SELECT and return all rows."""
    return conn.execute(sql, params).fetchall()


def count_rows_chunked(conn: sqlite3.Connection, table: str, column: str, ids: list[int]) -> int:
    """Count rows in `table` where `column` is in `ids`, chunked for SQLite."""
    if not ids:
        return 0
    total = 0
    for chunk in chunked(ids):
        placeholders = ",".join("?" * len(chunk))  # Builds: ?,?,?,?,...
        sql = f"SELECT COUNT(*) FROM {table} WHERE {column} IN ({placeholders})"
        total += conn.execute(sql, chunk).fetchone()[0]
    return total


def delete_chunked(conn: sqlite3.Connection, table: str, column: str, ids: list[int]) -> int:
    """
    Bulk-delete rows where `column` is in `ids`, chunked for SQLite.
    Returns total number of deleted rows.
    """
    if not ids:
        return 0
    total = 0
    for chunk in chunked(ids):
        placeholders = ",".join("?" * len(chunk))
        sql = f"DELETE FROM {table} WHERE {column} IN ({placeholders})"
        cursor = conn.execute(sql, chunk)
        total += cursor.rowcount
    return total


# ============================================================================
# Core deletion logic
# ============================================================================

def _delete_by_dataset_ids(
    conn: sqlite3.Connection,
    dataset_ids: list[int],
    dry_run: bool,
) -> dict:
    """
    Delete (or count) every record that belongs to the given dataset PKs.

    Deletion order (child → parent):
        TopologyFile / ParameterFile / TrajectoryFile → File → Dataset

    Returns a dict with row counts per table.
    """
    if not dataset_ids:
        return {}

    # ── Collect file PKs for these datasets ───────────────────────────────
    file_ids = []
    for chunk in chunked(dataset_ids):
        placeholders = ",".join("?" * len(chunk))
        rows = fetch_all(
            conn,
            f"SELECT file_id FROM files WHERE dataset_id IN ({placeholders})",
            chunk,
        )
        file_ids.extend(row["file_id"] for row in rows)

    counts = {}

    if dry_run:
        # Only COUNT — never DELETE
        counts["topology_files"]   = count_rows_chunked(conn, "topology_files",   "file_id", file_ids)
        counts["parameter_files"]  = count_rows_chunked(conn, "parameter_files",  "file_id", file_ids)
        counts["trajectory_files"] = count_rows_chunked(conn, "trajectory_files", "file_id", file_ids)
        counts["files"]            = len(file_ids)
        counts["datasets"]         = len(dataset_ids)
        return counts

    # ── Real deletions — deepest child tables first ────────────────────────
    counts["topology_files"]   = delete_chunked(conn, "topology_files",   "file_id",   file_ids)
    counts["parameter_files"]  = delete_chunked(conn, "parameter_files",  "file_id",   file_ids)
    counts["trajectory_files"] = delete_chunked(conn, "trajectory_files", "file_id",   file_ids)
    counts["files"]            = delete_chunked(conn, "files",            "dataset_id", dataset_ids)
    counts["datasets"]         = delete_chunked(conn, "datasets",         "dataset_id", dataset_ids)

    return counts


def _log_counts(counts: dict, dry_run: bool) -> None:
    """Pretty-print the deletion counts."""
    prefix = "[DRY-RUN] Would delete" if dry_run else "Deleted"
    for table, n in counts.items():
        print(f"  {prefix} {n:>7,} row(s) from {table}")


# ============================================================================
# Public entry points
# ============================================================================

def delete_dataset(source_name: str, id_in_source: str, dry_run: bool = False) -> None:
    """
    Remove a single dataset and all its related records.

    Args:
        source_name:   Repository name as stored in the DB, e.g. "zenodo".
        id_in_source:  The dataset's ID within that repository, e.g. "1234567".
        dry_run:       If True, only print what would be deleted.
    """
    print(f"INFO  | Mode: DELETE DATASET  |  source='{source_name}'  id='{id_in_source}'")
    if dry_run:
        print("WARN  | DRY-RUN enabled — no changes will be written to the database.")

    conn = get_connection()

    with conn:  # Context manager: commits on success, rolls back on error
        # 1. Resolve the data source
        source = fetch_one(
            conn,
            "SELECT data_source_id, name FROM data_sources WHERE name = ?",
            (source_name,),
        )
        if not source:
            print(f"ERROR | Data source '{source_name}' not found in the database.")
            sys.exit(1)

        # 2. Resolve the dataset
        dataset = fetch_one(
            conn,
            """
            SELECT dataset_id, title
            FROM datasets
            WHERE data_source_id = ? AND id_in_data_source = ?
            """,
            (source["data_source_id"], id_in_source),
        )
        if not dataset:
            print(f"ERROR | Dataset '{id_in_source}' from '{source_name}' not found.")
            sys.exit(1)

        print(f"INFO  | Found dataset PK={dataset['dataset_id']}  title='{dataset['title']}'")

        # 3. Delete everything (or simulate it)
        counts = _delete_by_dataset_ids(conn, [dataset["dataset_id"]], dry_run)

        if dry_run:
            conn.rollback()  # Undo any accidental changes
        else:
            # conn commits automatically when the `with` block exits cleanly
            print("OK    | Transaction committed.")

    _log_counts(counts, dry_run)


def delete_source(source_name: str, dry_run: bool = False) -> None:
    """
    Remove ALL datasets from a data source, then remove the source itself.

    Args:
        source_name:  Repository name as stored in the DB, e.g. "zenodo".
        dry_run:      If True, only print what would be deleted.
    """
    print(f"INFO  | Mode: DELETE SOURCE  |  source='{source_name}'")
    if dry_run:
        print("WARN  | DRY-RUN enabled — no changes will be written to the database.")

    conn = get_connection()

    with conn:
        # 1. Resolve the data source
        source = fetch_one(
            conn,
            "SELECT data_source_id, name FROM data_sources WHERE name = ?",
            (source_name,),
        )
        if not source:
            print(f"ERROR | Data source '{source_name}' not found in the database.")
            sys.exit(1)

        # 2. Collect all dataset PKs for this source
        rows = fetch_all(
            conn,
            "SELECT dataset_id FROM datasets WHERE data_source_id = ?",
            (source["data_source_id"],),
        )
        dataset_ids = [row["dataset_id"] for row in rows]
        print(f"INFO  | Found {len(dataset_ids):,} dataset(s) under '{source_name}'.")

        # 3. Confirmation prompt — safety gate for destructive operation
        if not dry_run:
            answer = input(
                f"\n  WARNING: This will permanently delete ALL {len(dataset_ids):,} datasets "
                f"and all related records for '{source_name}'.\n"
                f"  Type the source name to confirm: "
            ).strip()
            if answer != source_name:
                print("WARN  | Confirmation did not match. Aborting — nothing was deleted.")
                sys.exit(0)

        # 4. Delete all child records + datasets
        counts = _delete_by_dataset_ids(conn, dataset_ids, dry_run)

        # 5. Delete the data source row itself
        if not dry_run:
            conn.execute(
                "DELETE FROM data_sources WHERE data_source_id = ?",
                (source["data_source_id"],),
            )
            counts["data_sources"] = 1
            print("OK    | Transaction committed.")
        else:
            counts["data_sources"] = 1
            conn.rollback()

    _log_counts(counts, dry_run)


# ============================================================================
# CLI
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Remove datasets or entire data sources from the database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Delete one dataset (always dry-run first)
  python delete_data_sqlite3.py --dataset zenodo 1234567 --dry-run
  python delete_data_sqlite3.py --dataset zenodo 1234567

  # Delete all datasets from a source (requires confirmation prompt)
  python delete_data_sqlite3.py --source zenodo --dry-run
  python delete_data_sqlite3.py --source zenodo
        """,
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dataset",
        nargs=2,
        metavar=("SOURCE_NAME", "ID_IN_SOURCE"),
        help="Delete a single dataset by source name and its ID in that source.",
    )
    mode.add_argument(
        "--source",
        metavar="SOURCE_NAME",
        help="Delete ALL datasets from an entire data source.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be deleted without making any changes.",
    )

    args = parser.parse_args()
    start = time.perf_counter()

    if args.dataset:
        source_name, id_in_source = args.dataset
        delete_dataset(source_name, id_in_source, dry_run=args.dry_run)
    else:
        delete_source(args.source, dry_run=args.dry_run)

    elapsed = str(timedelta(seconds=time.perf_counter() - start)).split(".")[0]
    print(f"INFO  | Total time: {elapsed}")
    print("OK    | Done.")


if __name__ == "__main__":
    main()
