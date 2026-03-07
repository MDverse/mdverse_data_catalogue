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
    TopologyFile,
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

def load_topology_data(parquet_path_topology: str) -> pd.DataFrame:
    """Load parquet file and return DataFrame with selected columns.

    Rename columns to match the SQLModel table columns.
    """
    topology_df = pd.read_parquet(parquet_path_topology)

    topology_df = topology_df[[
        'dataset_origin',
        'dataset_id',
        'file_name',
        'atom_number',
        'has_protein',
        'has_nucleic',
        'has_lipid',
        'has_glucid',
        "has_water_ion"
        ]].rename(columns={
            'dataset_id': 'dataset_id_in_origin',
            'file_name': 'name'
            })

    return topology_df

# ============================================================================
# Helper functions
# ============================================================================

def delete_files(engine: Engine) -> None:
    "Delete all files in the TopologyFile table"
    with Session(engine) as session:
        topology_stmt = delete(TopologyFile)
        result_topology =session.exec(topology_stmt)

        session.commit()
    logger.info(f"Total rows from TOPOLOGY_FILES deleted: {result_topology.rowcount}\n")


# ============================================================================
# TopologyFile
# ============================================================================


def create_topology_table(
        topology_df: pd.DataFrame,
        engine: Engine,
        # datasets_ids_new_or_modified: list[int],
        ) -> None:
    """Create the TopologyFile records in the database."""

    with Session(engine) as session:
        for _, row in tqdm(
            topology_df.iterrows(),
            total=len(topology_df),
            desc="Processing topology rows",
            unit="row",
            ):

            dataset_id_in_origin = row["dataset_id_in_origin"]
            dataset_origin = row["dataset_origin"]
            statement_dataset = select(Dataset).join(DataSource).where(
                Dataset.id_in_data_source == dataset_id_in_origin,
                DataSource.name == dataset_origin
                )
            dataset_obj = session.exec(statement_dataset).first()
            if not dataset_obj:
                logger.debug(
                    f"Dataset with id_in_origin {dataset_id_in_origin}"
                    f" and origin {dataset_origin} not found."
                    )
                continue  # Skip if not found
            dataset_id = dataset_obj.dataset_id

            # # Determine if file exists already based on dataset status.
            # existing_file = True
            # # If the dataset is new or has been modified,
            # # then all files are new to the database
            # if dataset_id in datasets_ids_new_or_modified:
            #     existing_file = False
            
            # if not existing_file:

            gro_file_name = row["name"]
            statement_file = select(File).join(FileType).where(
                # Here we filter out the .gro files to go faster but when
                # we'll have more than just .gro files in the topology table,
                # we'll remove this or refine
                FileType.name == "gro",
                File.name == gro_file_name,
                File.dataset_id == dataset_id
            )
            files = session.exec(statement_file).all()
            if len(files) > 1:
                logger.debug(
                    f"Multiple files found with dataset_id {dataset_obj.dataset_id}"
                    f" and file name {gro_file_name}. Skipping..."
                    )
                # print(files)
                continue
            file_obj = session.exec(statement_file).first()
            if not file_obj:
                logger.debug(
                    f"File with dataset_id {dataset_obj.dataset_id}"
                    f" and file name {gro_file_name} not found."
                    )
                continue  # Skip if not found
            file_id_in_files = file_obj.file_id


            # -- Create the TopologyFile --
            topology_obj = TopologyFile(
                file_id=file_id_in_files,
                atom_number=row["atom_number"],
                has_protein=row["has_protein"],
                has_nucleic=row["has_nucleic"],
                has_lipid=row["has_lipid"],
                has_glucid=row["has_glucid"],
                has_water_ion=row["has_water_ion"]
            )

            session.add(topology_obj)
            session.commit()


def data_ingestion():
  
    start_1 = time.perf_counter()
    
    # Path to the parquet file
    gro_path = "data/parquet_files/gromacs_gro_files.parquet"

    topology_df = load_topology_data(gro_path)

    # Delete all files in the TopologyFile table
    delete_files(engine)
    

    # Ingest data in TopologyFile
    logger.info("Creating TopologyFile table...")
    create_topology_table(topology_df, engine)
    logger.success("Completed creating topology table.\n")
    
    
    # Measure the total execution time
    execution_time_1 = time.perf_counter() - start_1
    elapsed_time_1 = str(timedelta(seconds=execution_time_1)).split('.')[0]

    logger.info(f"Topology ingestion time: {elapsed_time_1}")
    logger.success("Topology ingestion complete.")


if __name__ == "__main__":
    data_ingestion()