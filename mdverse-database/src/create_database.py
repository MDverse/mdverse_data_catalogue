import time

from db_schema import create_db_and_tables

"""Purpose:
This script takes care of creating the database and tables.
We measure the script execution time in order to get an idea of how long it takes
to complete the data ingestion process.

To launch this script, use the command:
uv run python src/app.py
"""

start = time.perf_counter()


def main():
    # Create the database and tables
    create_db_and_tables()

if __name__ == "__main__":
    main()

execution_time = time.perf_counter() - start
print(f"Database and tables creation time: {execution_time:.2f} seconds")
