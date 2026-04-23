"""
Purpose:
    This script removes data from the database in two modes:

    1. DATASET mode  — removes a single dataset identified by its repository
                       name and its ID within that repository.
    2. SOURCE mode   — removes ALL datasets (and every related record) that
                       belong to a given data source (e.g. "zenodo").

How it works:
    Deletions always cascade from the deepest child tables up to the parent,
    respecting foreign-key constraints:

        TopologyFile  ─┐
        ParameterFile  ├─► File ──► Dataset ──► DataSource (source mode only)
        TrajectoryFile ─┘

    All deletions for a single run happen inside ONE transaction. If anything
    fails the entire transaction is rolled back automatically, so the database
    is never left in a half-deleted state.

Performance:
    All target IDs are collected with a single SELECT before any DELETE is
    issued. Each table is then wiped with one bulk DELETE ... WHERE id IN (...)
    statement — no Python loop, no per-row round-trips.

Usage:
    # Delete one dataset (repo name + its ID inside that repo)
    uv run delete_data.py --dataset zenodo 1234567

    # Delete ALL datasets from an entire repository
    # WARNING: irreversible — always dry-run first.
    uv run delete_data.py --source zenodo

    # Dry-run: prints what WOULD be deleted without touching the database
    # Always do this first to verify before a real deletion.
    uv run delete_data.py --dataset zenodo 1234567 --dry-run
    uv run delete_data.py --source zenodo --dry-run
"""

import sys
import argparse
import time
from datetime import timedelta
from pathlib import Path

from loguru import logger
from sqlmodel import Session, select, func
from sqlalchemy import delete

from db_schema import (
    engine,
    Dataset,
    DataSource,
    File,
    TopologyFile,
    ParameterFile,
    TrajectoryFile,
)


# ============================================================================
# Logger  (same style as ingest_data.py)
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
# Core deletion logic
# ============================================================================

SQLITE_MAX_VARS = 999  # SQLite hard limit on parameters per query


def _chunked(ids: list[int], size: int = SQLITE_MAX_VARS):
    """Yield successive chunks of `size` from a list of IDs."""
    for i in range(0, len(ids), size):
        yield ids[i : i + size]


def _count_rows(session: Session, Model, id_column, ids: list[int]) -> int:
    """Return the number of rows in Model where id_column is in ids.
    Splits into chunks to respect SQLite's variable limit."""
    if not ids:
        return 0
    total = 0
    for chunk in _chunked(ids):
        total += session.exec(
            select(func.count()).select_from(Model).where(id_column.in_(chunk))
        ).one()
    return total


def _chunked_delete(session, Model, id_column, ids: list[int]) -> int:
    """Bulk-delete rows where id_column is in ids, chunked for SQLite.
    Returns the total number of deleted rows."""
    if not ids:
        return 0
    total = 0
    for chunk in _chunked(ids):
        r = session.exec(delete(Model).where(id_column.in_(chunk)))
        total += r.rowcount
    return total


def _delete_by_dataset_ids(session: Session, dataset_ids: list[int], dry_run: bool) -> dict:
    """
    Delete every record that belongs to the given dataset PKs.

    Deletion order (child → parent):
        TopologyFile / ParameterFile / TrajectoryFile → File → Dataset

    All IN (...) clauses are chunked to stay within SQLite's 999-variable limit.
    Returns a dict with the row counts that were (or would be) deleted.
    """
    if not dataset_ids:
        return {}

    # ── Collect the file PKs that belong to these datasets ────────────────
    # Chunked to respect SQLite's variable limit.
    file_ids: list[int] = []
    for chunk in _chunked(dataset_ids):
        file_ids.extend(
            session.exec(
                select(File.file_id).where(File.dataset_id.in_(chunk))
            ).all()
        )

    counts = {}

    if dry_run:
        # In dry-run mode we only COUNT, never DELETE.
        counts["TopologyFile"]   = _count_rows(session, TopologyFile,   TopologyFile.file_id,   file_ids)
        counts["ParameterFile"]  = _count_rows(session, ParameterFile,  ParameterFile.file_id,  file_ids)
        counts["TrajectoryFile"] = _count_rows(session, TrajectoryFile, TrajectoryFile.file_id, file_ids)
        counts["File"]           = len(file_ids)
        counts["Dataset"]        = len(dataset_ids)
        return counts

    # ── Real deletions — deepest child tables first ────────────────────────
    counts["TopologyFile"]   = _chunked_delete(session, TopologyFile,   TopologyFile.file_id,   file_ids)
    counts["ParameterFile"]  = _chunked_delete(session, ParameterFile,  ParameterFile.file_id,  file_ids)
    counts["TrajectoryFile"] = _chunked_delete(session, TrajectoryFile, TrajectoryFile.file_id, file_ids)
    counts["File"]           = _chunked_delete(session, File,           File.dataset_id,         dataset_ids)
    counts["Dataset"]        = _chunked_delete(session, Dataset,        Dataset.dataset_id,      dataset_ids)

    return counts


def _log_counts(counts: dict, dry_run: bool) -> None:
    """Pretty-print the deletion counts."""
    prefix = "[DRY-RUN] Would delete" if dry_run else "Deleted"
    for table, n in counts.items():
        logger.info(f"  {prefix} {n:>7,} row(s) from {table}")


# ============================================================================
# Public entry points
# ============================================================================

def delete_dataset(source_name: str, id_in_source: str, dry_run: bool = False) -> None:
    """
    Remove a single dataset — identified by its repository name and the ID it
    has inside that repository — together with all its files and simulation
    records.

    Args:
        source_name:   Repository name as stored in the DB, e.g. "zenodo".
        id_in_source:  The dataset's ID within that repository, e.g. "1234567".
        dry_run:       If True, only print what would be deleted.
    """
    logger.info(f"Mode: DELETE DATASET  |  source='{source_name}'  id='{id_in_source}'")
    if dry_run:
        logger.warning("DRY-RUN enabled — no changes will be written to the database.")

    with Session(engine) as session:

        # 1. Resolve the DataSource row
        source = session.exec(
            select(DataSource).where(DataSource.name == source_name)
        ).first()

        if not source:
            logger.error(f"Data source '{source_name}' not found in the database.")
            sys.exit(1)

        # 2. Resolve the Dataset row
        dataset = session.exec(
            select(Dataset).where(
                Dataset.data_source_id == source.data_source_id,
                Dataset.id_in_data_source == id_in_source,
            )
        ).first()

        if not dataset:
            logger.error(
                f"Dataset '{id_in_source}' from '{source_name}' not found in the database."
            )
            sys.exit(1)

        logger.info(f"Found dataset PK={dataset.dataset_id}  title='{dataset.title}'")

        # 3. Delete everything (or simulate it)
        counts = _delete_by_dataset_ids(session, [dataset.dataset_id], dry_run)

        if not dry_run:
            session.commit()
            logger.success("Transaction committed.")
        else:
            session.rollback()

    _log_counts(counts, dry_run)


def delete_source(source_name: str, dry_run: bool = False) -> None:
    """
    Remove ALL datasets belonging to a data source, then remove the
    DataSource record itself.

    Args:
        source_name:  Repository name as stored in the DB, e.g. "zenodo".
        dry_run:      If True, only print what would be deleted.
    """
    logger.info(f"Mode: DELETE SOURCE  |  source='{source_name}'")
    if dry_run:
        logger.warning("DRY-RUN enabled — no changes will be written to the database.")

    with Session(engine) as session:

        # 1. Resolve the DataSource row
        source = session.exec(
            select(DataSource).where(DataSource.name == source_name)
        ).first()

        if not source:
            logger.error(f"Data source '{source_name}' not found in the database.")
            sys.exit(1)

        # 2. Collect all dataset PKs for this source
        dataset_ids: list[int] = list(
            session.exec(
                select(Dataset.dataset_id).where(
                    Dataset.data_source_id == source.data_source_id
                )
            ).all()
        )

        logger.info(f"Found {len(dataset_ids):,} dataset(s) under '{source_name}'.")

        # ── Confirmation prompt for destructive source-level deletion ─────
        if not dry_run:
            answer = input(
                f"\n  WARNING: This will permanently delete ALL {len(dataset_ids):,} datasets "
                f"and all related records for '{source_name}'.\n"
                f"  Type the source name to confirm: "
            ).strip()
            if answer != source_name:
                logger.warning("Confirmation did not match. Aborting — nothing was deleted.")
                sys.exit(0)

        # 3. Delete all child records + datasets
        counts = _delete_by_dataset_ids(session, dataset_ids, dry_run)

        # 4. Also delete the DataSource row itself
        if not dry_run:
            session.exec(
                delete(DataSource).where(DataSource.data_source_id == source.data_source_id)
            )
            counts["DataSource"] = 1
            session.commit()
            logger.success("Transaction committed.")
        else:
            counts["DataSource"] = 1  # Would delete 1 DataSource row
            session.rollback()

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
  # Delete one dataset
  uv run delete_data.py --dataset zenodo 1234567

  # Delete all datasets from a source
  uv run delete_data.py --source zenodo

  # Dry-run (safe preview — nothing is deleted)
  uv run delete_data.py --dataset zenodo 1234567 --dry-run
  uv run delete_data.py --source zenodo --dry-run
        """,
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dataset",
        nargs=2,
        metavar=("SOURCE_NAME", "ID_IN_SOURCE"),
        help="Delete a single dataset. Provide the repo name then its ID.",
    )
    mode.add_argument(
        "--source",
        metavar="SOURCE_NAME",
        help="Delete ALL datasets from an entire data source.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be deleted without making any changes.",
    )

    args = parser.parse_args()
    start = time.perf_counter()

    if args.dataset:
        source_name, id_in_source = args.dataset
        delete_dataset(source_name, id_in_source, dry_run=args.dry_run)
    else:
        delete_source(args.source, dry_run=args.dry_run)

    elapsed = str(timedelta(seconds=time.perf_counter() - start)).split(".")[0]
    logger.info(f"Total time: {elapsed}")
    logger.success("Done.")


if __name__ == "__main__":
    main()
