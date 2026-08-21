"""Tests for the MDverse DuckDB database module."""

from collections.abc import Generator

import duckdb
import pytest

from mdverse.core.logger import create_logger


@pytest.fixture
def memory_db() -> Generator[duckdb.DuckDBPyConnection]:
    """Provide an in-memory DuckDB connection for testing.

    Yields
    ------
        duckdb.DuckDBPyConnection: In-memory DuckDB connection.
    """
    conn = duckdb.connect(database=":memory:")
    yield conn
    conn.close()


def test_create_database_schema(
    memory_db: duckdb.DuckDBPyConnection,
    request: pytest.FixtureRequest,
) -> None:
    """Test DuckDB schema creation from the SQL query file."""
    logger = create_logger("logs/test_database.log")
    schema_path = request.config.rootpath / "params" / "queries" / "database_schema.sql"
    assert schema_path.exists(), f"Schema file not found at {schema_path}"
    sql_script = schema_path.read_text(encoding="utf-8")
    memory_db.execute(sql_script)
    tables = memory_db.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    # Log created tables
    logger.info(f"Number of tables created: {len(tables)}.")
    for table in tables:
        logger.info(f"Created table: {table[0]}")
    assert len(tables) > 0, "No tables were created from the schema file."
