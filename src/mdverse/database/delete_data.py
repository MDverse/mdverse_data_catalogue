"""Remove datasets or entire data sources from MDverse DuckDB database."""

import time
from datetime import timedelta
from pathlib import Path

import click
import duckdb
from loguru import logger

from mdverse.core.logger import create_logger
from mdverse.database.ingest_data import init_db_connection

TABLE_MAP = [
    ("molecules_external_databases", "molecule_id", "MoleculeExternalDB"),
    ("molecules", "annotation_id", "Molecule"),
    ("annotations", "dataset_id", "Annotation"),
    ("datasets_authors_link", "dataset_id", "DatasetAuthorLink"),
    ("files", "dataset_id", "File"),
    ("datasets", "dataset_id", "Dataset"),
]


def fetch_ids(conn: duckdb.DuckDBPyConnection, sql: str, params: list) -> list[int]:
    """Execute a query and return flat list of retrieved IDs.

    Returns
    -------
        list[int]: Flat list of IDs.
    """
    return [row[0] for row in conn.execute(sql, params).fetchall()]


def collect_ids(
    conn: duckdb.DuckDBPyConnection, dataset_ids: list[int]
) -> dict[str, list[int]]:
    """Collect related IDs for given dataset IDs in child-first dependency.

    Returns
    -------
        dict[str, list[int]]: Dictionary of collected target IDs.
    """
    if not dataset_ids:
        return {"dataset": [], "file": [], "annotation": [], "molecule": []}

    fmt = ",".join("?" * len(dataset_ids))
    file_ids = fetch_ids(
        conn,
        f"SELECT file_id FROM files WHERE dataset_id IN ({fmt})",  # noqa: S608
        dataset_ids,
    )
    ann_ids = fetch_ids(
        conn,
        f"SELECT annotation_id FROM annotations WHERE dataset_id IN ({fmt})",  # noqa: S608
        dataset_ids,
    )

    mol_ids = []
    if ann_ids:
        afmt = ",".join("?" * len(ann_ids))
        mol_ids = fetch_ids(
            conn,
            f"SELECT molecule_id FROM molecules WHERE annotation_id IN ({afmt})",  # noqa: S608
            ann_ids,
        )

    return {
        "dataset": dataset_ids,
        "file": file_ids,
        "annotation": ann_ids,
        "molecule": mol_ids,
    }


def query_ids_count(
    conn: duckdb.DuckDBPyConnection, table: str, col: str, ids: list[int]
) -> int:
    """Count rows matching given target IDs safely.

    Returns
    -------
        int: Matching row count.
    """
    if not ids:
        return 0
    fmt = ",".join("?" * len(ids))
    # Direct list unnesting pattern prevents dynamic string construction warnings.
    query = f"SELECT COUNT(*) FROM {table} WHERE {col} IN ({fmt})"  # noqa: S608
    return conn.execute(query, ids).fetchone()[0]


def delete_by_ids(
    conn: duckdb.DuckDBPyConnection, table: str, col: str, ids: list[int]
) -> int:
    """Delete rows matching given target IDs safely.

    Returns
    -------
        int: Total number of deleted rows.
    """
    if not ids:
        return 0
    fmt = ",".join("?" * len(ids))
    query = f"DELETE FROM {table} WHERE {col} IN ({fmt})"  # noqa: S608
    return conn.execute(query, ids).fetchone()[0]


def process_deletion(
    conn: duckdb.DuckDBPyConnection,
    targets: dict[str, list[int]],
    *,
    dry_run: bool,
) -> dict[str, int]:
    """Execute preview or child-first deletion across database tables.

    Returns
    -------
        dict[str, int]: Deleted row count mapping per entity.
    """
    counts = {}
    id_map = {
        "molecule_id": targets["molecule"],
        "annotation_id": targets["annotation"],
        "dataset_id": targets["dataset"],
        "file_id": targets["file"],
    }

    for table, col, label in TABLE_MAP:
        ids = id_map[col]
        if dry_run:
            counts[label] = query_ids_count(conn, table, col, ids)
            continue

        if table == "files" and ids:
            fmt = ",".join("?" * len(ids))
            cnt = 0
            for clause in (
                "parent_zip_file_id IS NOT NULL",
                "parent_zip_file_id IS NULL",
            ):
                query = f"DELETE FROM files WHERE file_id IN ({fmt}) AND {clause}"  # noqa: S608
                cnt += conn.execute(query, ids).fetchone()[0]
            counts[label] = cnt
        else:
            counts[label] = delete_by_ids(conn, table, col, ids)

    return counts


def run_deletion(
    db_path: Path, source_name: str, id_in_source: str | None, *, dry_run: bool
) -> None:
    """Orchestrate source or dataset deletion workflow."""
    with init_db_connection(db_path) as conn:
        src = conn.execute(
            "SELECT data_source_label FROM data_sources WHERE data_source_label = ?",
            [source_name],
        ).fetchone()
        if not src:
            logger.error(f"Data source '{source_name}' not found.")
            return

        if id_in_source:
            ds = conn.execute(
                "SELECT dataset_id FROM datasets "
                "WHERE data_source_label = ? AND id_in_data_source = ?",
                [source_name, id_in_source],
            ).fetchone()
            if not ds:
                logger.error(f"Dataset '{id_in_source}' not found in '{source_name}'.")
                return
            ds_ids = [ds[0]]
        else:
            ds_ids = fetch_ids(
                conn,
                "SELECT dataset_id FROM datasets WHERE data_source_label = ?",
                [source_name],
            )

        if not dry_run and not id_in_source:
            msg = f"Type '{source_name}' to confirm full repository deletion: "
            if input(msg).strip() != source_name:
                logger.warning("Confirmation mismatch. Aborting.")
                return

        targets = collect_ids(conn, ds_ids)
        counts = process_deletion(conn, targets, dry_run)

        if not dry_run and not id_in_source:
            conn.execute(
                "DELETE FROM data_sources WHERE data_source_label = ?",
                [source_name],
            )
            counts["DataSource"] = 1

        prefix = "[DRY-RUN] Would delete" if dry_run else "Deleted"
        for label, count in counts.items():
            logger.info(f"{prefix} {count:>7,} row(s) from {label}")


@click.command(help="Delete dataset or data source records from DuckDB.")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True, file_okay=True, path_type=Path),
    required=True,
    help="Path to DuckDB database file.",
)
@click.option(
    "--datarepo",
    "source_name",
    required=True,
    help="Target repository name (e.g. zenodo, atlas).",
)
@click.option(
    "--dataset",
    "id_in_source",
    default=None,
    help="Specific dataset ID in repository.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Preview deleted row counts without mutating data.",
)
def main(
    db_path: Path, source_name: str, id_in_source: str | None, *, dry_run: bool
) -> None:
    """CLI entry point for data deletion."""
    logger = create_logger(Path("logs/delete_data.log"))
    start = time.perf_counter()
    run_deletion(db_path, source_name, id_in_source, dry_run)
    elapsed = str(timedelta(seconds=time.perf_counter() - start)).split(".")[0]
    logger.info(f"Total time: {elapsed}")
    logger.success("Done.")


if __name__ == "__main__":
    main()
