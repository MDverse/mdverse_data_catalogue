"""
report_duckdb.py
---------
Report row and column counts for all tables in the MDverse DuckDB database.

Usage:
    uv run report_duckdb.py
"""

import sys
import argparse
from pathlib import Path

import duckdb
from loguru import logger


DB_PATH = Path(__file__).parent / "database.duckdb"

TABLES = [
    "data_sources",
    "datasets",
    "datasets_authors_link",
    "authors",
    "files",
    "file_types",
    "topology_files",
    "parameter_files",
    "trajectory_files",
    "projects",
    "papers",
    "authors_papers_link",
    "annotations",
    "annotation_types",
    "provenance_types",
    "molecules",
    "molecule_types",
    "molecules_external_db",
    "databases",
]

logger.remove()
logger.add(sys.stderr,
    format="{time:MMMM D, YYYY - HH:mm:ss} | <lvl>{level:<8} | {message}</lvl>",
    level="DEBUG")
logger.add(f"{Path(__file__).stem}.log", mode="w",
    format="{time:YYYY-MM-DDTHH:mm:ss} | <lvl>{level:<8} | {message}</lvl>",
    level="DEBUG")


def main(db_path: Path) -> None:
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        logger.error("Run create_database.py first.")
        sys.exit(1)

    conn = duckdb.connect(str(db_path), read_only=True)

    col_counts: dict[str, int] = {
        row[0]: row[1] for row in conn.execute("""
            SELECT table_name, COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = 'main'
            GROUP BY table_name
        """).fetchall()
    }

    logger.info(f"Database : {db_path.resolve()}")
    logger.info("-" * 62)
    logger.info(f"{'Table':<28} {'Columns':>7}  {'Rows':>12}")
    logger.info("-" * 62)

    total = 0
    for table in TABLES:
        n_rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        total += n_rows
        logger.info(f"{table:<28} {col_counts.get(table, 0):>7,}  {n_rows:>12,}")

    logger.info("-" * 62)
    logger.info(f"{'TOTAL ROWS':<28} {'':>7}  {total:>12,}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Report row and column counts for all MDverse tables.")
    parser.add_argument("--db", metavar="PATH", default=str(DB_PATH),
        help=f"Path to the DuckDB database file (default: {DB_PATH}).")
    main(Path(parser.parse_args().db))