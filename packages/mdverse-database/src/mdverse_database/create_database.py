"""Create MDverse datalogue database and tables."""

import time

from .database import create


def main():
    """Create the database and tables."""
    start = time.perf_counter()
    create()
    execution_time = time.perf_counter() - start
    print(f"Database and tables created in {execution_time:.2f} seconds")


if __name__ == "__main__":
    main()
