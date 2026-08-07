"""Module for loading SQL queries from the app/queries directory."""

from functools import lru_cache
from pathlib import Path

QUERIES_DIR = Path("webapp/app/queries")


@lru_cache
def load_query(query_str_path: str) -> str:
    """Load and cache an SQL query from the app/queries directory."""
    query_path = QUERIES_DIR / query_str_path
    if not query_path.exists():
        raise FileNotFoundError(
            f"Query file '{query_str_path}' not found in {QUERIES_DIR}."
        )
    return query_path.read_text(encoding="utf-8")
