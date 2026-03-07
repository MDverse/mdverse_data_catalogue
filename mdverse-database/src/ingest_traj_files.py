import sys
import time
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import Engine
from sqlmodel import Session, select, delete
from tqdm import tqdm

from datetime import datetime
from datetime import timedelta
from db_schema import (
    engine,
    Dataset,
    DataSource,
    File,
    FileType,
    TrajectoryFile,
)



# ============================================================================
# Logger configuration
# ============================================================================

logger.remove()
# Terminal format
logger.add(
    sys.stderr,
    format="{time:MMMM D, YYYY - HH:mm:ss} | <lvl>{level:<8} | {message}</lvl>",
    level="DEBUG",
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

# ============================================================================
# Data loading functions
# ============================================================================

def load_trajectory_data(parquet_path_trajectory: str) -> pd.DataFrame:
    """Load parquet file and return DataFrame with selected columns.

    Rename columns to match the SQLModel table columns.
    """
    trajectory_df = pd.read_parquet(parquet_path_trajectory)

    trajectory_df = trajectory_df[[
    'dataset_origin',
    'dataset_id',
    'file_name',
    'atom_number',
    'frame_number'
    ]].rename(columns={
        'dataset_origin': 'data_source',
        'dataset_id': 'dataset_id_in_origin',
        'file_name': 'name'
        })

    return trajectory_df

# ============================================================================
# Helper functions
# ============================================================================

def delete_files(engine: Engine) -> None:
    "Delete all files in the TrajectoryFile table"
    with Session(engine) as session:
        trajectory_stmt = delete(TrajectoryFile)
        result_trajectory = session.exec(trajectory_stmt)

        session.commit()
    logger.info(f"Total rows from TRAJECTORY_FILES deleted: {result_trajectory.rowcount}\n")


# ============================================================================
# TrajectoryFile
# ============================================================================


def create_trajectory_table(
        traj_df: pd.DataFrame,
        engine: Engine,
        # datasets_ids_new_or_modified: list[int],
        ) -> None:
    """Create the TrajectoryFile records in the database."""

    with Session(engine) as session:
        missing_files = 0
        for index, row in tqdm(
            traj_df.iterrows(),
            total=len(traj_df),
            desc="Processing trajectory rows",
            unit="row",
            ):
            xtc_file_name = row["name"]

            dataset_id_in_origin = row["dataset_id_in_origin"]
            dataset_origin = row["data_source"]
            statement = select(Dataset).join(DataSource).where(
                Dataset.id_in_data_source == dataset_id_in_origin,
                DataSource.name == dataset_origin
                )
            dataset_obj = session.exec(statement).first()
            if not dataset_obj:
                logger.debug(
                    f"Dataset with id_in_origin {dataset_id_in_origin}"
                    f" and origin {dataset_origin} not found."
                    f"Skipping {xtc_file_name} (index: {index})..."
                    )
                missing_files += 1
                continue  # Skip if not found
            dataset_id = dataset_obj.dataset_id


            statement_file = select(File).join(FileType).where(
                # Here we filter out the .xtc files to go faster but when
                # we'll have more than just .xtc files in the trajectory table,
                # we'll remove this or refine
                FileType.name == "xtc",
                File.name == xtc_file_name,
                File.dataset_id == dataset_id
            )
            files = session.exec(statement_file).all()
            if len(files) > 1:
                logger.debug(
                    f"Multiple files found with dataset_id {dataset_obj.dataset_id}"
                    f" and file name {xtc_file_name}. Skipping..."
                    )
                # print(files)
                continue
            file_obj = session.exec(statement_file).first()
            if not file_obj:
                logger.debug(
                    f"File with dataset_id {dataset_obj.dataset_id}"
                    f" and file name {xtc_file_name} not found.\n",
                    f"Skipping {xtc_file_name} (index: {index})..."
                    )
                missing_files += 1
                continue  # Skip if not found
            file_id_in_files = file_obj.file_id


            # -- Create the TrajectoryFile --
            traj_obj = TrajectoryFile(
                file_id=file_id_in_files,
                atom_number=row["atom_number"],
                frame_number=row["frame_number"]
            )

            session.add(traj_obj)
            session.commit()

    logger.debug(f"Number of missing files: {missing_files}")

def data_ingestion():
  
    start_1 = time.perf_counter()
    
    # Path to the parquet file
    xtc_path = "data/parquet_files/gromacs_xtc_files.parquet"

    trajectory_df = load_trajectory_data(xtc_path)

    # Delete all files in the TrajectoryFile table
    delete_files(engine)
    

    # Ingest data in TrajectoryFile
    logger.info("Creating TrajectoryFile table...")
    create_trajectory_table(trajectory_df, engine)
    logger.success("Completed creating trajectory table.\n")
    
    
    # Measure the total execution time
    execution_time_1 = time.perf_counter() - start_1
    elapsed_time_1 = str(timedelta(seconds=execution_time_1)).split('.')[0]

    logger.info(f"Trajectory ingestion time: {elapsed_time_1}")
    logger.success("Trajectory ingestion complete.")


if __name__ == "__main__":
    data_ingestion()