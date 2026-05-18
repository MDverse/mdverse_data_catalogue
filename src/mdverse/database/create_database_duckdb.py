"""
create_database_duckdb.py
------------------
Creates the MDverse DuckDB database from database_schema_duckdb.sql.

Usage:
    uv run create_database_duckdb.py --db database.duckdb --schema database_schema_duckdb.sql
"""

import duckdb
import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",     required=True, help="Path to the DuckDB database file.")
    parser.add_argument("--schema", required=True, help="Path to the SQL schema file.")
    args = parser.parse_args()

    schema_sql = Path(args.schema).read_text()

    conn = duckdb.connect(args.db)
    conn.execute(schema_sql)
    conn.close()

    print(f"OK | Database created: {args.db}")


if __name__ == "__main__":
    main()