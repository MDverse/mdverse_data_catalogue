"""Create MDverse datalogue database and tables."""

import time

from .db_schema import create_db_and_tables


def main():
    """Create the database and tables."""
    start = time.perf_counter()
    create_db_and_tables()
    execution_time = time.perf_counter() - start
    print(f"Database and tables created in {execution_time:.2f} seconds")


if __name__ == "__main__":
    main()
