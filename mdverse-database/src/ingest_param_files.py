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
    ParameterFile,
)
from ingest_data import (
    get_or_create_models_with_one_attribute,
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

def load_parameter_data(parquet_path_parameters: str) -> pd.DataFrame:
    """Load parquet file and return DataFrame with selected columns.

    Rename columns to match the SQLModel table columns.
    """
    parameter_df = pd.read_parquet(parquet_path_parameters)

    parameter_df = parameter_df[[
        'dataset_origin',
        'dataset_id',
        'file_name',
        'dt',
        'nsteps',
        'temperature',
        'thermostat',
        'barostat',
        'integrator'
        ]].rename(columns={
            'dataset_id': 'dataset_id_in_origin',
            'file_name': 'name'
            })
    
    # If integrator is missing, set it no 'unknown
    parameter_df['integrator'] = parameter_df['integrator'].apply(lambda x: x if pd.notna(x) else "undefined")

    return parameter_df


# ============================================================================
# Helper functions
# ============================================================================

def delete_files(engine: Engine) -> None:
    "Delete all files in the ParameterFile table"
    with Session(engine) as session:
        parameter_stmt = delete(ParameterFile)
        result_parameter =session.exec(parameter_stmt)

        session.commit()
    logger.info(f"Total rows from PARAMETER_FILES deleted: {result_parameter.rowcount}\n")


# ============================================================================
# Parameter,File, Thermostat, Barostat, Integrator
# ============================================================================

def create_parameters_table(
        param_df: pd.DataFrame,
        engine: Engine,
        # datasets_ids_new_or_modified: list[int],
        ) -> None:
    """
    Create the ParameterFile records in the database.
    At the same time, create the Thermostat, Barostat, and Integrator records.
    """

    with Session(engine) as session:
        for _, row in tqdm(
            param_df.iterrows(),
            total=len(param_df),
            desc="Processing parameter rows",
            unit="row",
        ):

            dataset_id_in_origin = row["dataset_id_in_origin"]
            dataset_origin = row["dataset_origin"]
            statement = select(Dataset).join(DataSource).where(
                Dataset.id_in_data_source == dataset_id_in_origin,
                DataSource.name == dataset_origin
                )
            dataset_obj = session.exec(statement).first()
            if not dataset_obj:
                logger.debug(
                    f"Dataset with id_in_origin {dataset_id_in_origin}"
                    f" and origin {dataset_origin} not found."
                    )
                continue  # Skip if not found
            dataset_id = dataset_obj.dataset_id


            mdp_file_name = row["name"]
            statement_file = select(File).join(FileType).where(
                # Here we filter out the .mdp files to go faster but when
                # we'll have more than just .mdp files in the parameters table,
                # we'll remove this or refine
                FileType.name == "mdp",
                File.name == mdp_file_name,
                File.dataset_id == dataset_id
            )
            files = session.exec(statement_file).all()
            if len(files) > 1:
                logger.debug(
                    f"Multiple files found with dataset_id {dataset_obj.dataset_id}"
                    f" and file name {mdp_file_name}. Skipping..."
                    )
                # print(files)
                continue
            file_obj = session.exec(statement_file).first()
            if not file_obj:
                logger.debug(
                    f"File with dataset_id {dataset_obj.dataset_id}"
                    f" and file name {mdp_file_name} not found."
                    )
                continue  # Skip if not found
            file_id_in_files = file_obj.file_id


            # -- Create the ParameterFile --
            parameter_obj = ParameterFile(
                file_id=file_id_in_files,
                dt=row["dt"],
                nsteps=row["nsteps"],
                temperature=row["temperature"],
                thermostat=row.get("thermostat", None),
                barostat=row.get("barostat", None),
                integrator=row.get("integrator", None)
            )

            session.add(parameter_obj)
            session.commit()


def data_ingestion():
  
    start_1 = time.perf_counter()
    
    # Path to the parquet file
    mdp_path = "data/parquet_files/gromacs_mdp_files.parquet"

    parameter_df = load_parameter_data(mdp_path)

    # Delete all files in the TopologyFile table
    delete_files(engine)
    

    # Ingest data in TopologyFile
    logger.info("ParameterFile, Thermostat, Barostat, Integrator tables...")
    create_parameters_table(parameter_df, engine)
    logger.success("Completed creating parameters tables.\n")
    
    
    # Measure the total execution time
    execution_time_1 = time.perf_counter() - start_1
    elapsed_time_1 = str(timedelta(seconds=execution_time_1)).split('.')[0]

    logger.info(f"Parameter ingestion time: {elapsed_time_1}")
    logger.success("Parameter ingestion complete.")


if __name__ == "__main__":
    data_ingestion()