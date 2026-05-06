"""
create_database.py
------------------
Creates the MDverse SQLite database from database_schema.sql.

Usage:
    uv run create_database.py --db database.db --schema database_schema.sql
"""

import re
import sqlite3
import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",     required=True, help="Path to the SQLite database file.")
    parser.add_argument("--schema", required=True, help="Path to the SQL schema file.")
    args = parser.parse_args()

    sql = Path(args.schema).read_text()

    # Create the database
    conn = sqlite3.connect(args.db)
    conn.executescript(sql)
    conn.close()

    # Parse tables and columns directly from the .sql file
    # (avoids sqlite_master, sqlite_schema, PRAGMA — none of which work in DuckDB)
    tables = {}
    for block in re.finditer(r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(\w+)\s*\((.*?)\);", sql, re.S | re.I):
        table, body = block.group(1), block.group(2)
        tables[table] = [
            line.strip().rstrip(",").split()[0]
            for line in body.splitlines()
            if line.strip() and not re.match(r"(PRIMARY|FOREIGN|UNIQUE|CHECK|CONSTRAINT)", line.strip(), re.I)
        ]

    # SHOW TABLES
    print("\nSHOW TABLES")
    print("=" * 32)
    for name in sorted(tables):
        print(f"  {name}")
    print("-" * 32)
    print(f"  {len(tables)} table(s)\n")

    # DESCRIBE <table>
    for table, columns in sorted(tables.items()):
        print(f"DESCRIBE {table}")
        print("=" * 40)
        for i, col in enumerate(columns, 1):
            print(f"  {i:<4} {col}")
        print(f"  {len(columns)} column(s)\n")


if __name__ == "__main__":
    main()