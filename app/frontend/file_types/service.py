"""SQL queries for file types"""

import pandas as pd
from sqlalchemy import extract, func, case, desc
from sqlalchemy.orm import selectinload, aliased
from sqlmodel import Session, select, or_, col
from typing import Optional

from ...db_schema import (
    Dataset,
    DataSource,
    File,
    FileType,
    ParameterFile,
    TopologyFile,
    TrajectoryFile,
    engine,
)


def get_file_types_stats():
    """
    Retrieves statistics for each file type, including:
    - the number of files per file type,
    - the number of datasets per file type,
    - the total size of files per file type in gigabytes.

    Returns:
    file_type_stats_summary (list): A list of results where
    each result is a row containing:
        - file_type (str): The name of the file type.
        - number_of_files (int): The count of files for this file type.
        - number_of_datasets (int): The count of datasets containing
                                    files of this file type.
        - total_size_in_GB (float): The total size of files for this
                                    file type in gigabytes.
    """
    with Session(engine) as session:
        statement = (
            select(
            FileType.name.label("file_type"),
            func.count(File.file_id).label("number_of_files"),
            func.count(func.distinct(Dataset.dataset_id)).label("number_of_datasets"),
            (func.sum(File.size_in_bytes) / 1e9).label("total_size_in_GB"),
            )
            .join(File, File.file_type_id == FileType.file_type_id)
            .outerjoin(Dataset, Dataset.dataset_id == File.dataset_id)
            .group_by(FileType.name)
            .order_by(func.count(func.distinct(File.file_id)).desc())
        )
        file_type_stats_summary = session.exec(statement).all()
        return file_type_stats_summary


def get_list_of_files_for_a_file_type(file_type: str) -> pd.DataFrame:
    """
    Returns a DataFrame with all files of a given file type.
    """
    # Create an alias for the parent file
    ParentFile = aliased(File)
    
    # Define a CASE expression: if file is from a zip, then use parent's URL, else use file.url
    file_url_expr = case(
        (File.is_from_zip_file == True, ParentFile.url),
        else_=File.url
    ).label("file_url")

    with Session(engine) as session:
        statement = (
            select(
                Dataset.id_in_data_source.label("dataset_id"),
                DataSource.name.label("dataset_origin"),
                File.name.label("file_name"),
                File.size_in_bytes.label("file_size_in_bytes"),
                File.is_from_zip_file.label("is_file_from_zip_file"),
                file_url_expr,
                Dataset.url_in_data_source.label("dataset_url"),
            )
            .join(FileType, File.file_type_id == FileType.file_type_id)
            .join(Dataset, File.dataset_id == Dataset.dataset_id)
            .join(DataSource, Dataset.data_source_id == DataSource.data_source_id)
            # Left join the parent file so that files from a zip can retrieve the parent URL.
            .join(ParentFile, File.parent_zip_file_id == ParentFile.file_id, isouter=True)
            .where(FileType.name == file_type)
        )
        results = session.exec(statement).all()
        # Convert results to a list of dictionaries then to a DataFrame
        data = [dict(row._mapping) for row in results]
        df = pd.DataFrame(data)
        return df

def get_gro_files_for_datatables(
    dataset_id: int | None = None,
    sort_column_name: str | None = None,
    sort_direction: str | None = "asc",
    start: int | None = None,
    length: int | None = None,
    search: str | None = None,
    ) -> list[TopologyFile]:
    """
    Returns a list of GRO files with their related File, Dataset, and Dataset.origin info.
    
    If a dataset_id is provided, only GRO files for this dataset are returned.
    Otherwise, all GRO files are returned.
    """
    statement = (
        select(
            TopologyFile.atom_number.label("atom_number"),
            TopologyFile.has_protein.label("has_protein"),
            TopologyFile.has_nucleic.label("has_nucleic"),
            TopologyFile.has_lipid.label("has_lipid"),
            TopologyFile.has_glucid.label("has_glucid"),
            TopologyFile.has_water_ion.label("has_water_ion"),
            File.name.label("file_name"),
            Dataset.id_in_data_source.label("dataset_id_in_origin"),
            Dataset.url_in_data_source.label("dataset_url"),
            DataSource.name.label("dataset_origin"),
        )
        .join(File, TopologyFile.file_id == File.file_id)
        .join(Dataset, File.dataset_id == Dataset.dataset_id)
        .join(DataSource, Dataset.data_source_id == DataSource.data_source_id)
    )

    if dataset_id is not None:
        statement = statement.where(TopologyFile.file.has(File.dataset_id == dataset_id))

    if sort_column_name is not None:
        if sort_direction == "asc":
            statement = statement.order_by(sort_column_name)
        elif sort_direction == "desc":
            statement = statement.order_by(desc(sort_column_name))

    if start is not None:
        statement = statement.offset(start)

    if length is not None:
        statement = statement.limit(length)

    if search is not None:
        statement = statement.where(or_(
            DataSource.name.ilike(f"%{search}%"),
            Dataset.id_in_data_source.ilike(f"%{search}%"),
            File.name.ilike(f"%{search}%"),
            TopologyFile.atom_number.ilike(f"%{search}%"),
            TopologyFile.has_protein.ilike(f"%{search}%"),
            TopologyFile.has_nucleic.ilike(f"%{search}%"),
            TopologyFile.has_lipid.ilike(f"%{search}%"),
            TopologyFile.has_glucid.ilike(f"%{search}%"),
            TopologyFile.has_water_ion.ilike(f"%{search}%")
        ))
    with Session(engine) as session:
        results = session.exec(statement).all()
        return results


def get_mdp_files_for_datatables(
    dataset_id: int | None = None,
    sort_column_name: str | None = None,
    sort_direction: str | None = "asc",
    start: int | None = None,
    length: int | None = None,
    search: str | None = None,
    ) -> list[ParameterFile]:
    """
    Returns a list of MDP files with their related File, Dataset, and Dataset.origin info.
    
    If a dataset_id is provided, only MDP files for this dataset are returned.
    Otherwise, all MDP files are returned.
    """
    statement = (
        select(
            ParameterFile.dt.label("dt"),
            ParameterFile.nsteps.label("nsteps"),
            ParameterFile.temperature.label("temperature"),
            ParameterFile.thermostat,
            ParameterFile.barostat,
            ParameterFile.integrator,
            File.name.label("file_name"),
            Dataset.id_in_data_source.label("dataset_id_in_origin"),
            Dataset.url_in_data_source.label("dataset_url"),
            DataSource.name.label("dataset_origin"),
        )
        .join(File, ParameterFile.file_id == File.file_id)
        .join(Dataset, File.dataset_id == Dataset.dataset_id)
        .join(DataSource, Dataset.data_source_id == DataSource.data_source_id)
    )

    if dataset_id is not None:
        statement = statement.where(ParameterFile.file.has(File.dataset_id == dataset_id))

    if sort_column_name is not None:
        if sort_direction == "asc":
            statement = statement.order_by(sort_column_name)
        elif sort_direction == "desc":
            statement = statement.order_by(desc(sort_column_name))

    if start is not None:
        statement = statement.offset(start)

    if length is not None:
        statement = statement.limit(length)

    if search is not None:
        statement = statement.where(or_(
            DataSource.name.ilike(f"%{search}%"),
            Dataset.id_in_data_source.ilike(f"%{search}%"),
            File.name.ilike(f"%{search}%"),
            ParameterFile.thermostat.ilike(f"%{search}%"),
            ParameterFile.barostat.ilike(f"%{search}%"),
            ParameterFile.integrator.ilike(f"%{search}%"),
        ))

    with Session(engine) as session:
        results = session.exec(statement).all()
        return results

def get_xtc_files_for_datatables(
    dataset_id: int | None = None,
    sort_column_name: str | None = None,
    sort_direction: str | None = "asc",
    start: int | None = None,
    length: int | None = None,
    search: str | None = None,
    ) -> list[ParameterFile]:
    """
    Returns a list of XTC files with their related File, Dataset, and Dataset.origin info.
    
    If a dataset_id is provided, only XTC files for this dataset are returned.
    Otherwise, all XTC files are returned.
    """
    statement = (
        select(
            TrajectoryFile.atom_number,
            TrajectoryFile.frame_number,
            File.name.label("file_name"),
            Dataset.id_in_data_source.label("dataset_id_in_origin"),
            Dataset.url_in_data_source.label("dataset_url"),
            DataSource.name.label("dataset_origin"),
        )
        .join(File, TrajectoryFile.file_id == File.file_id)
        .join(Dataset, File.dataset_id == Dataset.dataset_id)
        .join(DataSource, Dataset.data_source_id == DataSource.data_source_id)
    )

    if dataset_id is not None:
        statement = statement.where(TrajectoryFile.file.has(File.dataset_id == dataset_id))

    if sort_column_name is not None:
        if sort_direction == "asc":
            statement = statement.order_by(sort_column_name)
        elif sort_direction == "desc":
            statement = statement.order_by(desc(sort_column_name))

    if start is not None:
        statement = statement.offset(start)

    if length is not None:
        statement = statement.limit(length)

    if search is not None:
        statement = statement.where(or_(
            DataSource.name.ilike(f"%{search}%"),
            Dataset.id_in_data_source.ilike(f"%{search}%"),
            File.name.ilike(f"%{search}%"),
            TrajectoryFile.atom_number.ilike(f"%{search}%"),
            TrajectoryFile.frame_number.ilike(f"%{search}%"),
        ))

    with Session(engine) as session:
        results = session.exec(statement).all()
        return results
