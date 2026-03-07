from pathlib import Path

from loguru import logger
from sqlalchemy import func
from sqlmodel import Session, select

from src.db_schema import (
    Author,
    Dataset,
    DataSource,
    File,
    FileType,
    ParameterFile,
    TopologyFile,
    TrajectoryFile,
    engine,
)

# Log file format
# Log file will be erased at each run
# Remove mode="w" to keep log file between runs
logger.add(
    f"{Path(__file__).stem}.log",
    mode="w",
    format="{time:YYYY-MM-DDTHH:mm:ss} | <lvl>{level:<8} | {message}</lvl>",
    level="DEBUG",
)


def main():
    """
    This script reports the number of rows and columns in each table of the database.
    To run this script, use the command:
    uv run python report.py
    """
    # List of all the models you want to report on.
    models = [
        Dataset,
        DataSource,
        Author,
        File,
        FileType,
        TopologyFile,
        ParameterFile,
        TrajectoryFile,
    ]

    with Session(engine) as session:
        for model in models:
            # Get the table name from the model's __tablename__ attribute.
            table_name = model.__tablename__
            # Count the number of columns using the model's table metadata.
            n_columns = len(model.__table__.columns)
            # Build a SQL query to count the rows in the table.
            # select(func.count()) creates a query that returns the count of rows.
            # .select_from(model) specifies the table (model) to count rows from.
            statement = select(func.count()).select_from(model)
            n_rows = session.exec(statement).first()
            # Log the table name, number of rows, and number of columns.
            logger.info(
                f"Table: {table_name:>16} - Columns: {n_columns:5} - Rows: {n_rows:10,}"
            )


if __name__ == "__main__":
    main()
