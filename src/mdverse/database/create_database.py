"""Creates the MDverse DuckDB database using the provided SQL schema."""

from pathlib import Path

import click
import duckdb
import loguru

from mdverse.core.logger import create_logger


def create_database(
    db_path: Path, schema_path: Path, logger: "loguru.Logger" = loguru.logger
) -> None:
    """Create the MDverse database using the provided SQL schema."""
    logger.info(f"Read SQL schema: {schema_path}")
    schema_sql = schema_path.read_text(encoding="utf-8")
    conn = duckdb.connect(db_path)
    conn.execute(schema_sql)
    conn.close()
    logger.info(f"Database created: {db_path}")


@click.command(
    help="Create the MDverse database.",
)
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=False, path_type=Path),
    required=True,
    help="Path to the DuckDB database file.",
)
@click.option(
    "--schema",
    "schema_path",
    type=click.Path(exists=True, file_okay=True, path_type=Path),
    required=True,
    help="Path to the SQL schema file.",
)
def main(db_path: Path, schema_path: Path) -> None:
    """Create database using the provided SQL schema."""
    logger = create_logger(logpath="logs/create_database.log", level="INFO")
    create_database(db_path=db_path, schema_path=schema_path, logger=logger)


if __name__ == "__main__":
    main()
