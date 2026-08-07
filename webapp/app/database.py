"""Connect to the DuckDB database and"""

from typing import Annotated, TypeAlias

import duckdb
from fastapi import Depends
from fastapi.templating import Jinja2Templates

DB_PATH = "data/database.duckdb"


def get_db():
    with duckdb.connect(DB_PATH, read_only=True) as conn:
        yield conn


# Define a type alias for the DuckDB connection dependency
ConnDep: TypeAlias = Annotated[duckdb.DuckDBPyConnection, Depends(get_db)]
# Define a Jinja2Templates instance for rendering HTML templates
templates = Jinja2Templates(directory="webapp/templates")
