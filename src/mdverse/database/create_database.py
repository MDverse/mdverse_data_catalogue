"""
create_database.py
------------------
Creates the MDverse SQLite database from database_schema.sql.

Usage:
    uv run create_database.py --db database.db --schema database_schema.sql
"""

import sqlite3
import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",     required=True, help="Path to the SQLite database file.")
    parser.add_argument("--schema", required=True, help="Path to the SQL schema file.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.executescript(Path(args.schema).read_text())
    conn.close()
    print(f"OK | Database created: {args.db}")


if __name__ == "__main__":
    main()
