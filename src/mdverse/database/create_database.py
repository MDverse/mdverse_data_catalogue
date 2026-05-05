"""
create_database.py
------------------
Creates the MDverse SQLite database from database_schema.sql.

All SQL lives in database_schema.sql — this script is a pure Python runner.

Usage:
    uv run create_database.py
"""

import sys
import sqlite3
import argparse
from pathlib import Path

# ============================================================================
# Configuration
# ============================================================================
DB_PATH     = Path(__file__).parent / "database.db"
SCHEMA_FILE = Path(__file__).parent / "database_schema.sql"


# ============================================================================
# Connection
# ============================================================================
def get_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ============================================================================
# Read database_schema.sql
# ============================================================================
def read_schema(schema_file: Path) -> str:
    if not schema_file.exists():
        print(f"ERROR | Schema file not found: {schema_file}")
        sys.exit(1)
    return schema_file.read_text(encoding="utf-8")


# ============================================================================
# Execute ALL SQL instructions in one single call
# ============================================================================
def execute_schema(conn: sqlite3.Connection, sql: str) -> None:
    try:
        conn.executescript(sql)
    except sqlite3.Error as exc:
        print(f"ERROR | {exc}")
        sys.exit(1)

# ============================================================================
# SHOW TABLES — list all user-defined tables currently in the database.
# ============================================================================
def show_tables(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT   name
        FROM     sqlite_master
        WHERE    type = 'table'
        AND      name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()

    print("\nSHOW TABLES")
    print("=" * 32)
    for (name,) in rows:
        print(f"  {name}")
    print("-" * 32)
    print(f"  {len(rows)} table(s)\n")

# ============================================================================
# DESCRIBE — show column details for every table using PRAGMA table_info().
# ============================================================================
def describe_tables(conn: sqlite3.Connection) -> None:
    tables = conn.execute(
        """
        SELECT   name
        FROM     sqlite_master
        WHERE    type = 'table'
        AND      name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()

    for (table_name,) in tables:
        columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()

        print(f"DESCRIBE {table_name}")
        print("=" * 65)
        print(f"  {'#':<5} {'Column':<30} {'Type':<12} {'Not Null':<10} {'Default':<10} {'PK'}")
        print("-" * 65)
        for cid, name, col_type, notnull, dflt_value, pk in columns:
            print(
                f"  {cid:<5} {name:<30} {col_type:<12}"
                f" {'YES' if notnull else '':<10}"
                f" {str(dflt_value) if dflt_value is not None else '':<10}"
                f" {'YES' if pk else ''}"
            )
        print(f"  {len(columns)} column(s)\n")

# ============================================================================
# CLI
# ============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create the MDverse SQLite database from database_schema.sql."
    )
    parser.add_argument(
        "--db", metavar="PATH", default=str(DB_PATH),
        help=f"Path to the SQLite database file (default: {DB_PATH})",
    )
    parser.add_argument(
        "--schema", metavar="PATH", default=str(SCHEMA_FILE),
        help=f"Path to the SQL schema file (default: {SCHEMA_FILE})",
    )
    args    = parser.parse_args()
    db_path = Path(args.db)
    schema  = Path(args.schema)

    print(f"INFO  | Database : {db_path.resolve()}")
    print(f"INFO  | Schema   : {schema.resolve()}")

    conn = get_connection(db_path)
    try:
        sql = read_schema(schema)
        execute_schema(conn, sql)
        print("OK    | All SQL instructions executed successfully.")
        show_tables(conn)
        describe_tables(conn)
        print("OK    | Done.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()