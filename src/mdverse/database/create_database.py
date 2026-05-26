"""
create_database.py
------------------
Creates the MDverse DuckDB database from database_schema.sql.

Usage:
    uv run create_database.py --db database.duckdb
    uv run create_database.py --db database.duckdb --schema params/database_schema.sql
"""
import duckdb
import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, help="Path to the DuckDB database file.")
    parser.add_argument(
        "--schema",
        default=str(Path(__file__).parent.parent.parent.parent / "params" / "database_schema.sql"),
        help="Path to the SQL schema file (default: params/database_schema.sql).",
    )
    args = parser.parse_args()
    schema_sql = Path(args.schema).read_text()
    conn = duckdb.connect(args.db)
    conn.execute(schema_sql)
    conn.close()
    print(f"OK | Database created: {args.db}")


if __name__ == "__main__":
    main()