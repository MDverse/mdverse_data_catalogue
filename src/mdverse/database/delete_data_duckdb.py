"""
delete_data_duckdb.py
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
    DuckDB v1.x enforces FK constraints per-statement even inside BEGIN/COMMIT,
    making transactional multi-table cascades impossible with FK constraints.
    The standard workaround is autocommit with strict child-first deletion order,
    which guarantees no orphaned rows at any point. Each DELETE is individually
    atomic; a mid-run failure leaves the database consistent and retryable.

Usage:
    uv run delete_data_duckdb.py --datarepo zenodo --dry-run
    uv run delete_data_duckdb.py --datarepo zenodo
    uv run delete_data_duckdb.py --datarepo zenodo --dataset <id_in_data_source> --dry-run
    uv run delete_data_duckdb.py --datarepo zenodo --dataset <id_in_data_source>
"""


import sys
import argparse
import time
from datetime import timedelta
from pathlib import Path

import duckdb


# ── Configuration ──────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "database.duckdb"


# ── Connection ─────────────────────────────────────────────────────────────────

def get_connection() -> duckdb.DuckDBPyConnection:
    if not DB_PATH.exists():
        print(f"ERROR | Database not found: {DB_PATH}")
        sys.exit(1)
    return duckdb.connect(str(DB_PATH))


# ── Helpers ────────────────────────────────────────────────────────────────────

def fetch_one(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None):
    return conn.execute(sql, params or []).fetchone()


# ── SQL subquery fragments ─────────────────────────────────────────────────────
#
# DATASET mode  — $1 = dataset_id  (INTEGER)
# SOURCE mode   — $1 = source_name (VARCHAR)
#
# No intermediate ID lists are ever materialised in Python.
# All subqueries are resolved entirely inside DuckDB.

# dataset mode subqueries
_D_DS = "SELECT $1::INTEGER"
_D_FI = f"SELECT file_id       FROM files       WHERE dataset_id    IN ({_D_DS})"
_D_AN = f"SELECT annotation_id FROM annotations WHERE dataset_id    IN ({_D_DS})"
_D_MO = f"SELECT molecule_id   FROM molecules   WHERE annotation_id IN ({_D_AN})"

# source mode subqueries
_S_DS = "SELECT dataset_id FROM datasets WHERE data_source_id = (SELECT data_source_id FROM data_sources WHERE name = $1)"
_S_FI = f"SELECT file_id       FROM files       WHERE dataset_id    IN ({_S_DS})"
_S_AN = f"SELECT annotation_id FROM annotations WHERE dataset_id    IN ({_S_DS})"
_S_MO = f"SELECT molecule_id   FROM molecules   WHERE annotation_id IN ({_S_AN})"


# ── Core deletion logic ────────────────────────────────────────────────────────

def _count(conn: duckdb.DuckDBPyConnection, param: list, ds: str, fi: str, an: str, mo: str) -> dict[str, int]:
    """Return row counts per table for dry-run preview. No data is modified."""

    def n(sql: str) -> int:
        return conn.execute(sql, param).fetchone()[0]

    return {
        "MoleculeExternalDB": n(f"SELECT COUNT(*) FROM molecules_external_db  WHERE molecule_id   IN ({mo})"),
        "Molecule":           n(f"SELECT COUNT(*) FROM molecules               WHERE annotation_id IN ({an})"),
        "Annotation":         n(f"SELECT COUNT(*) FROM annotations             WHERE dataset_id    IN ({ds})"),
        "TopologyFile":       n(f"SELECT COUNT(*) FROM topology_files          WHERE file_id       IN ({fi})"),
        "ParameterFile":      n(f"SELECT COUNT(*) FROM parameter_files         WHERE file_id       IN ({fi})"),
        "TrajectoryFile":     n(f"SELECT COUNT(*) FROM trajectory_files        WHERE file_id       IN ({fi})"),
        "DatasetAuthorLink":  n(f"SELECT COUNT(*) FROM datasets_authors_link   WHERE dataset_id    IN ({ds})"),
        "File":               n(f"SELECT COUNT(*) FROM files                   WHERE dataset_id    IN ({ds})"),
        "Dataset":            n(f"SELECT COUNT(*) FROM datasets                WHERE dataset_id    IN ({ds})"),
    }


def _delete(conn: duckdb.DuckDBPyConnection, param: list, ds: str, fi: str, an: str, mo: str) -> dict[str, int]:
    """
    Delete all related records in child-first order.
    Each statement runs in autocommit — see module docstring for rationale.
    """
    counts: dict[str, int] = {}

    def run(label: str, count_sql: str, delete_sql: str) -> None:
        counts[label] = conn.execute(count_sql, param).fetchone()[0]
        conn.execute(delete_sql, param)

    # 1. molecules_external_db — deepest child in annotation chain
    run("MoleculeExternalDB",
        f"SELECT COUNT(*) FROM molecules_external_db WHERE molecule_id   IN ({mo})",
        f"DELETE FROM molecules_external_db            WHERE molecule_id   IN ({mo})",
    )
    # 2. molecules
    run("Molecule",
        f"SELECT COUNT(*) FROM molecules WHERE annotation_id IN ({an})",
        f"DELETE FROM molecules            WHERE annotation_id IN ({an})",
    )
    # 3. annotations
    run("Annotation",
        f"SELECT COUNT(*) FROM annotations WHERE dataset_id IN ({ds})",
        f"DELETE FROM annotations            WHERE dataset_id IN ({ds})",
    )
    # 4. simulation-file metadata
    for table, label in [
        ("topology_files",   "TopologyFile"),
        ("parameter_files",  "ParameterFile"),
        ("trajectory_files", "TrajectoryFile"),
    ]:
        run(label,
            f"SELECT COUNT(*) FROM {table} WHERE file_id IN ({fi})",
            f"DELETE FROM {table}            WHERE file_id IN ({fi})",
        )
    # 5. dataset–author links
    run("DatasetAuthorLink",
        f"SELECT COUNT(*) FROM datasets_authors_link WHERE dataset_id IN ({ds})",
        f"DELETE FROM datasets_authors_link            WHERE dataset_id IN ({ds})",
    )
    # 6. files — two passes for self-referencing parent_zip_file_id FK
    total_files = 0
    for filt in ("parent_zip_file_id IS NOT NULL", "parent_zip_file_id IS NULL"):
        total_files += conn.execute(
            f"SELECT COUNT(*) FROM files WHERE dataset_id IN ({ds}) AND {filt}", param
        ).fetchone()[0]
        conn.execute(
            f"DELETE FROM files WHERE dataset_id IN ({ds}) AND {filt}", param
        )
    counts["File"] = total_files

    # 7. datasets
    run("Dataset",
        f"SELECT COUNT(*) FROM datasets WHERE dataset_id IN ({ds})",
        f"DELETE FROM datasets            WHERE dataset_id IN ({ds})",
    )

    return counts


def _log_counts(counts: dict[str, int], dry_run: bool) -> None:
    prefix = "[DRY-RUN] Would delete" if dry_run else "Deleted"
    for label, n in counts.items():
        print(f"  {prefix} {n:>7,} row(s) from {label}")


# ── Public entry points ────────────────────────────────────────────────────────

def delete_dataset(source_name: str, id_in_source: str, dry_run: bool = False) -> None:
    """Remove a single dataset and all its related records."""
    print(f"INFO  | Mode: DELETE DATASET  |  datarepo='{source_name}'  dataset='{id_in_source}'")
    if dry_run:
        print("WARN  | DRY-RUN — no changes will be written.")

    conn = get_connection()

    source = fetch_one(conn, "SELECT data_source_id FROM data_sources WHERE name = $1", [source_name])
    if not source:
        print(f"ERROR | Data source '{source_name}' not found.")
        conn.close(); sys.exit(1)

    dataset = fetch_one(
        conn,
        "SELECT dataset_id, title FROM datasets WHERE data_source_id = $1 AND id_in_data_source = $2",
        [source[0], id_in_source],
    )
    if not dataset:
        print(f"ERROR | Dataset '{id_in_source}' not found in '{source_name}'.")
        conn.close(); sys.exit(1)

    print(f"INFO  | Found dataset PK={dataset[0]}  title='{dataset[1]}'")
    param = [dataset[0]]

    if dry_run:
        counts = _count(conn, param, _D_DS, _D_FI, _D_AN, _D_MO)
    else:
        try:
            counts = _delete(conn, param, _D_DS, _D_FI, _D_AN, _D_MO)
            print("OK    | Deletion complete.")
        except Exception as exc:
            print(f"ERROR | Deletion failed: {exc}")
            conn.close(); sys.exit(1)

    conn.close()
    _log_counts(counts, dry_run)


def delete_source(source_name: str, dry_run: bool = False) -> None:
    """Remove ALL datasets belonging to a data source, then the source itself."""
    print(f"INFO  | Mode: DELETE SOURCE  |  datarepo='{source_name}'")
    if dry_run:
        print("WARN  | DRY-RUN — no changes will be written.")

    conn = get_connection()

    source = fetch_one(conn, "SELECT data_source_id FROM data_sources WHERE name = $1", [source_name])
    if not source:
        print(f"ERROR | Data source '{source_name}' not found.")
        conn.close(); sys.exit(1)

    dataset_count = fetch_one(
        conn,
        "SELECT COUNT(*) FROM datasets WHERE data_source_id = $1",
        [source[0]],
    )[0]
    print(f"INFO  | Found {dataset_count:,} dataset(s) under '{source_name}'.")

    param = [source_name]

    if dry_run:
        counts = _count(conn, param, _S_DS, _S_FI, _S_AN, _S_MO)
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
            print("WARN  | Confirmation did not match. Aborting.")
            sys.exit(0)

        conn = get_connection()
        try:
            counts = _delete(conn, param, _S_DS, _S_FI, _S_AN, _S_MO)
            conn.execute("DELETE FROM data_sources WHERE name = $1", param)
            counts["DataSource"] = 1
            print("OK    | Deletion complete.")
        except Exception as exc:
            print(f"ERROR | Deletion failed: {exc}")
            print("WARN  | Re-run with --dry-run to check remaining data.")
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
            "  uv run delete_data_duckdb.py --datarepo zenodo --dry-run\n"
            "  uv run delete_data_duckdb.py --datarepo zenodo\n"
            "  uv run delete_data_duckdb.py --datarepo zenodo --dataset <ID_IN_SOURCE> --dry-run\n"
            "  uv run delete_data_duckdb.py --datarepo zenodo --dataset <ID_IN_SOURCE>"
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
    print(f"INFO  | Total time: {elapsed}")
    print("OK    | Done.")


if __name__ == "__main__":
    main()