"""
Purpose:
    Remove data from the database in two modes:

    1. DATASET mode — removes a single dataset identified by its repository
                      name (--datarepo) and its ID within that repository
                      (--dataset).
    2. SOURCE  mode — removes ALL datasets (and every related record) that
                      belong to a given data source; omit --dataset to
                      trigger this mode.

How it works:
    Deletions always cascade from the deepest child tables up to the parent,
    respecting foreign-key constraints:

        TopologyFile    ─┐
        ParameterFile   ├─► File ──┐
        TrajectoryFile  ─┘         ├─► Dataset ──► DataSource (source mode only)
        DatasetAuthorLink ─────────┘

    All deletions for a single run happen inside ONE transaction. If anything
    fails the entire transaction is rolled back automatically, so the database
    is never left in a half-deleted state.

Performance:
    All target IDs are collected with a single SELECT before any DELETE is
    issued. Each table is then wiped with one bulk DELETE ... WHERE id IN (...)
    statement — no Python loop, no per-row round-trips.

Usage:
    # Delete an entire data source (always dry-run first)
    uv run delete_data.py --datarepo zenodo --dry-run
    uv run delete_data.py --datarepo zenodo

    # Delete a single dataset within a source (always dry-run first)
    uv run delete_data.py --datarepo zenodo --dataset 1234567 --dry-run
    uv run delete_data.py --datarepo zenodo --dataset 1234567
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
    DatasetAuthorLink,
    File,
    TopologyFile,
    ParameterFile,
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


# ============================================================================
# Core deletion logic
# ============================================================================

SQLITE_MAX_VARS = 999  # SQLite hard limit on parameters per query


def _chunked(ids: list[int], size: int = SQLITE_MAX_VARS):
    """Yield successive chunks of `size` from a list of IDs."""
    for i in range(0, len(ids), size):
        yield ids[i : i + size]


def _count_rows(session: Session, Model, id_column, ids: list[int]) -> int:
    """Count rows in Model where id_column is in ids (chunked for SQLite)."""
    if not ids:
        return 0
    total = 0
    for chunk in _chunked(ids):
        total += session.exec(
            select(func.count()).select_from(Model).where(id_column.in_(chunk))
        ).one()
    return total


def _chunked_delete(session: Session, Model, id_column, ids: list[int]) -> int:
    """Bulk-delete rows where id_column is in ids (chunked for SQLite).
    Returns total number of deleted rows."""
    if not ids:
        return 0
    total = 0
    for chunk in _chunked(ids):
        result = session.exec(delete(Model).where(id_column.in_(chunk)))
        total += result.rowcount
    return total


def _delete_by_dataset_ids(session: Session, dataset_ids: list[int], dry_run: bool) -> dict:
    """
    Delete (or count) every record that belongs to the given dataset PKs.

    Deletion order (child → parent):
        TopologyFile / ParameterFile / TrajectoryFile
            → DatasetAuthorLink (datasets_authors_link)
            → File
            → Dataset

    Returns a dict with row counts per table.
    """
    if not dataset_ids:
        return {}

    # Collect file PKs for these datasets (chunked for SQLite)
    file_ids: list[int] = []
    for chunk in _chunked(dataset_ids):
        file_ids.extend(
            session.exec(
                select(File.file_id).where(File.dataset_id.in_(chunk))
            ).all()
        )

    if dry_run:
        return {
            "TopologyFile":          _count_rows(session, TopologyFile,      TopologyFile.file_id,        file_ids),
            "ParameterFile":         _count_rows(session, ParameterFile,     ParameterFile.file_id,       file_ids),
            "TrajectoryFile":        _count_rows(session, TrajectoryFile,    TrajectoryFile.file_id,      file_ids),
            "DatasetAuthorLink":     _count_rows(session, DatasetAuthorLink, DatasetAuthorLink.dataset_id, dataset_ids),
            "File":                  len(file_ids),
            "Dataset":               len(dataset_ids),
        }

    return {
        "TopologyFile":          _chunked_delete(session, TopologyFile,      TopologyFile.file_id,        file_ids),
        "ParameterFile":         _chunked_delete(session, ParameterFile,     ParameterFile.file_id,       file_ids),
        "TrajectoryFile":        _chunked_delete(session, TrajectoryFile,    TrajectoryFile.file_id,      file_ids),
        "DatasetAuthorLink":     _chunked_delete(session, DatasetAuthorLink, DatasetAuthorLink.dataset_id, dataset_ids),
        "File":                  _chunked_delete(session, File,              File.dataset_id,              dataset_ids),
        "Dataset":               _chunked_delete(session, Dataset,           Dataset.dataset_id,           dataset_ids),
    }


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
    Remove a single dataset — identified by --datarepo and --dataset — together
    with all its files and simulation records.
    """
    logger.info(f"Mode: DELETE DATASET  |  datarepo='{source_name}'  dataset='{id_in_source}'")
    if dry_run:
        logger.warning("DRY-RUN enabled — no changes will be written to the database.")

    with Session(engine) as session:
        source = session.exec(
            select(DataSource).where(DataSource.name == source_name)
        ).first()
        if not source:
            logger.error(f"Data source '{source_name}' not found in the database.")
            sys.exit(1)

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

        counts = _delete_by_dataset_ids(session, [dataset.dataset_id], dry_run)

        if dry_run:
            session.rollback()
        else:
            session.commit()
            logger.success("Transaction committed.")

    _log_counts(counts, dry_run)


def delete_source(source_name: str, dry_run: bool = False) -> None:
    """
    Remove ALL datasets belonging to a data source, then the DataSource row
    itself.
    """
    logger.info(f"Mode: DELETE SOURCE  |  datarepo='{source_name}'")
    if dry_run:
        logger.warning("DRY-RUN enabled — no changes will be written to the database.")

    with Session(engine) as session:
        source = session.exec(
            select(DataSource).where(DataSource.name == source_name)
        ).first()
        if not source:
            logger.error(f"Data source '{source_name}' not found in the database.")
            sys.exit(1)

        dataset_ids: list[int] = list(
            session.exec(
                select(Dataset.dataset_id).where(
                    Dataset.data_source_id == source.data_source_id
                )
            ).all()
        )
        logger.info(f"Found {len(dataset_ids):,} dataset(s) under '{source_name}'.")

        # Confirmation prompt — before any writes
        if not dry_run:
            answer = input(
                f"\n  WARNING: This will permanently delete ALL {len(dataset_ids):,} datasets "
                f"and all related records for '{source_name}'.\n"
                f"  Type the source name to confirm: "
            ).strip()
            if answer != source_name:
                logger.warning("Confirmation did not match. Aborting — nothing was deleted.")
                sys.exit(0)

        counts = _delete_by_dataset_ids(session, dataset_ids, dry_run)

        if dry_run:
            counts["DataSource"] = 1
            session.rollback()
        else:
            session.exec(
                delete(DataSource).where(
                    DataSource.data_source_id == source.data_source_id
                )
            )
            counts["DataSource"] = 1
            session.commit()
            logger.success("Transaction committed.")

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
  # Delete an entire data source (always dry-run first)
  uv run delete_data.py --datarepo zenodo --dry-run
  uv run delete_data.py --datarepo zenodo

  # Delete a single dataset within a source (always dry-run first)
  uv run delete_data.py --datarepo zenodo --dataset 1234567 --dry-run
  uv run delete_data.py --datarepo zenodo --dataset 1234567
        """,
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
        help="ID of a single dataset within the repository. "
             "Omit to delete ALL datasets in --datarepo.",
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