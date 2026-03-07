import sys
import time
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import Engine
from sqlmodel import Session, select, delete, SQLModel
from tqdm import tqdm

from datetime import datetime
from datetime import timedelta
from db_schema import (
    engine,
    Author,
    Dataset,
    DataSource,
    File,
    FileType,
    ParameterFile,
    TopologyFile,
    TrajectoryFile,
)


"""Purpose:
This script takes care of transforming the data from the parquet files
into the SQLModel objects. We will create the following functions:
- create_datasets_authors_origins
- create_files

How it works:
- We load the data from the parquet files into pandas DataFrames.
- We rename the columns to match the SQLModel table columns.
- We transform the data into the SQLModel objects.
- We create the objects in the database.

To launch this script, use the command:
uv run python src/ingest_data.py
"""

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
# Helper functions
# ============================================================================

def get_or_create_models_with_one_attribute(
    session: Session,
    Model: SQLModel,
    attribute: str,
    value: str,
    additional_data: dict = None
    ) -> SQLModel:
    # We create a SQL statement to find the records in Model
    # with the same name as in the current row. This could give us
    # multiple rows from Model, but we only need one (.first()).
    # The result will be an instance of Model if the value
    # already exists, or None if it doesn't.
    statement = select(Model).where(getattr(Model, attribute) == value)
    result = session.exec(statement).first()
    # If the value doesn't exist, we create a new one
    # in the Model table and commit the changes.
    if not result:
        # Prepare the data for the new instance
        data = {attribute: value}
        if additional_data:
            data.update(additional_data)
        result = Model(**data)
        session.add(result)
        session.commit()
        session.refresh(result)
    return result

def update_dataset_fields(existing: Dataset, row: pd.Series, fields: list[str]) -> bool:
    """Compare and update fields; return True if any field has changed."""
    changed = False
    for field in fields:
        # "new_value" is the value from the current row in the DataFrame
        # It doesn't mean that it's new, it's just the value we're
        # comparing to the existing dataset.
        new_value = row[field]
        if getattr(existing, field) != new_value: # If the field has changed
            setattr(existing, field, new_value) # Update it
            # This is equivalent to writing existing_dataset.field if we knew
            # the field name ahead of time, but since field is a variable,
            # getattr is used
            changed = True
    return changed


def delete_files_for_update(engine: Engine, new_or_modified_datasets: list[int]) -> None:
    "Delete all files associated to the datasets that have been updated"
    with Session(engine) as session:
        files_stmt = delete(File).where(File.dataset_id.in_(new_or_modified_datasets))
        result_files = session.exec(files_stmt)

        # For the following tables, they do not have a dataset_id. Their file_id is
        # all we have. Meaning that the file_id goes back to the File table that does
        # have a dataset_id. This is too complicated to base ourselves off of for deletion.
        # We will delete all the rows in these tables then add again the data.

        # WARNING: This is with the assumption that the parquet files used to ingest the data
        # will always have ALL the files, not just new ones or a select amount of the old ones.
        trajectory_stmt = delete(TrajectoryFile)
        result_trajectory= session.exec(trajectory_stmt)

        parameter_stmt = delete(ParameterFile)
        result_parameter =session.exec(parameter_stmt)

        topology_stmt = delete(TopologyFile)
        result_topology =session.exec(topology_stmt)

        session.commit()

    logger.info(f"Total rows from FILES deleted from updated datasets: {result_files.rowcount}")
    logger.info(f"Total rows from TRAJECTORY_FILES deleted: {result_trajectory.rowcount}")
    logger.info(f"Total rows from PARAMETER_FILES deleted: {result_parameter.rowcount}")
    logger.info(f"Total rows from TOPOLOGY_FILES deleted: {result_topology.rowcount}\n")


# ============================================================================
# Data loading functions
# ============================================================================

def load_datasets_data(parquet_path: str) -> pd.DataFrame:
    datsets_df = pd.read_parquet(parquet_path)
    datsets_df = datsets_df[[
        'dataset_origin',
        'dataset_id',
        'doi',
        'date_creation',
        'date_last_modified',
        'date_fetched',
        'file_number',
        'download_number',
        'view_number',
        'license',
        'dataset_url',
        'title',
        'author',
        'keywords',
        'description'
    ]].rename(columns={
        'dataset_origin': 'data_source',
        'dataset_id': 'id_in_data_source',
        'date_creation': 'date_created',
        'date_fetched': 'date_last_crawled',
        'dataset_url': 'url_in_data_source'
    })

    # Normalize author and keywords strings
    # For datasets["author"] column, we can have multiple authors
    # separated by a comma or a semicolon (replace semicolon with comma).
    # We need to split the authors but keep them concatenated.
    # We simply need to remove the space after the comma.
    datsets_df['author'] = datsets_df['author'].str.replace(", ", ",").str.replace(";", ",")
    datsets_df['keywords'] = datsets_df['keywords'].str.replace(", ", ",").str.replace("; ", ";").str.replace(",", ";")

    # We want to make all keywords lowercase in order to avoid duplicates
    # when we create the Keyword objects in the database.
    datsets_df["keywords"] = datsets_df["keywords"].str.lower()

    # Normally we'd expect all datasets to have at least one author, but it
    # seems that datasets from OSF might not have an author field.
    # We need to account for that by replacing NaN with an empty string.
    datsets_df["author"] = datsets_df["author"].apply(lambda x: x if pd.notna(x) else "")
    datsets_df["keywords"] = datsets_df["keywords"].apply(lambda x: x if pd.notna(x) else "")

    # The column 'data_source' is the name of the data source
    # We want to add a new column "data_source_url" that will contain the URL
    # of the data source.
    # If the row is "zenodo", the URL will be "https://zenodo.org/"
    # If the row is "figshare", the URL will be "https://figshare.com/"
    # If the row is "osf", the URL will be "https://osf.io/"
    datsets_df['data_source_url'] = datsets_df['data_source'].map({
        'zenodo': 'https://zenodo.org/',
        'figshare': 'https://figshare.com/',
        'osf': 'https://osf.io/'
    })

    return datsets_df

def load_files_data(parquet_path: str) -> pd.DataFrame:
    files_df = pd.read_parquet(parquet_path)
    files_df = files_df[[
        'dataset_origin',
        'dataset_id',
        'file_type',
        'file_name',
        'file_size',
        'file_md5',
        'file_url',
        'from_zip_file',
        'origin_zip_file'
    ]].rename(columns={
        'dataset_origin': 'data_source',
        'dataset_id': 'dataset_id_in_data_source',
        'file_type': 'type',
        'file_name': 'name',
        'file_size': 'size_in_bytes',
        'file_md5': 'md5',
        'file_url': 'url',
        'from_zip_file': 'is_from_zip_file',
        'origin_zip_file': 'parent_zip_file_name'
    })
    return files_df

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
            'dataset_origin': 'data_source',
            'dataset_id': 'dataset_id_in_data_source',
            'file_name': 'name'
            })

    return topology_df

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
            'dataset_origin': 'data_source',
            'dataset_id': 'dataset_id_in_data_source',
            'file_name': 'name'
            })
    
    # If integrator is missing, set it no 'unknown
    parameter_df['integrator'] = parameter_df['integrator'].apply(lambda x: x if pd.notna(x) else "undefined")

    return parameter_df

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
        'dataset_id': 'dataset_id_in_data_source',
        'file_name': 'name'
        })

    return trajectory_df


# ============================================================================
# Dataset, Author, DataSource
# ============================================================================

def create_or_update_datasets_authors_origins_tables(
        datasets_df: pd.DataFrame,
        engine: Engine,
        ) -> list[int]:
    """
    Create or update dataset-related tables in the database.

    This function processes a DataFrame containing dataset information and
    updates the database accordingly. It handles the creation and updating
    of Dataset, Author, DataSource, and Keyword entries.

    Args:
        datasets_df (pd.DataFrame): DataFrame containing dataset information.
        engine (Engine): SQLAlchemy Engine instance to connect to the database.

    Returns:
        None

    Process:
        - Inserts new records into the Dataset, Author, DataSource,
        and Keyword tables.
        - Updates existing records if changes are detected.
        - Logs the number of records created, updated, or ignored.

    """
    total_rows = len(datasets_df)

    # Dataset ids for new, unchanged, and modified datasets
    datasets_ids_new = []
    datasets_ids_unchanged = []
    datasets_ids_modified = []

    # The session is used to interact with the database—querying, adding,
    # and committing changes.
    with Session(engine) as session:
        for _, row in tqdm(
            datasets_df.iterrows(),
            total=total_rows,
            desc="Processing rows",
            unit="row"
            ):

            # --- Handle DataSource (one-to-many relationship) ---
            origin_name = row["data_source"]
            origin_url = row["data_source_url"]
            dict_to_add_data_source = {
                "url": origin_url,
                "citation": None,  # CURRENTLY we don't have a citation
                "comment": None,  # CURRENTLY we don't have a comment
            }
            origin_obj = get_or_create_models_with_one_attribute(
                session, DataSource, "name", origin_name, dict_to_add_data_source)
            


            # --- Handle Author(s) (many-to-many relationship) ---
            # If there are multiple authors separated by a delimiter (","),
            # split and process them accordingly.
            # This also removes any leading/trailing whitespace from each name
            author_names = [name.strip() for name in row["author"].split(",") if name.strip()]
            # if name.strip() condition ensures that only non-empty
            # strings are included in the final list.
            # If a substring is empty or consists solely of whitespace,
            # name.strip() will evaluate to an empty string,
            # which is considered False in a boolean context,
            # and thus it will be excluded from the resulting list

            # After we have an Author object (either retrieved from the
            # database or newly created), we use the following command to
            # add the Author object to our authors list (list of Author
            # objects for the current dataset).
            dict_to_add_author = {
                "orcid": None,  # CURRENTLY we don't have an ORCID
            }
            authors = [get_or_create_models_with_one_attribute(session, Author, "name", name, dict_to_add_author) for name in author_names]


            # --- Check if the Dataset already exists ---
            # Uniqueness is determined by (origin, id_in_data_source)
            dataset_statement = select(Dataset).where(
                (Dataset.id_in_data_source == row["id_in_data_source"]) &
                (Dataset.data_source_id == origin_obj.data_source_id)
            )
            existing_dataset = session.exec(dataset_statement).first()

            if not existing_dataset: # If the dataset doesn't exist, create it.
            # --- Create the new Dataset entry ---
                new_dataset_obj = Dataset(
                    id_in_data_source=row["id_in_data_source"],
                    doi=row["doi"],
                    date_created=row["date_created"],
                    date_last_modified=row["date_last_modified"],
                    date_last_crawled=row["date_last_crawled"],
                    file_number=row["file_number"],
                    download_number=row["download_number"],
                    view_number=row["view_number"],
                    license=row["license"],
                    url_in_data_source=row["url_in_data_source"],
                    title=row["title"],
                    # use .get() if the field might be missing
                    keywords=row.get("keywords"),
                    description=row.get("description"),
                    data_source=origin_obj,   # assign the related origin
                )

                # Assign the many-to-many relationship for authors:
                # In our Dataset model, we have defined an attribute called author
                # that represents a MtM relationship with the Author model.
                # When we write the following, we are assigning the list of Author
                # objects (collected in the authors list) to the dataset’s author
                # attribute. This informs SQLModel/SQLAlchemy to create the
                # appropriate link table entries so that the dataset is
                # related to all these authors.
                new_dataset_obj.author = authors

                # # Assign the many-to-many relationship for keywords:
                # new_dataset_obj.keyword = keyword_entries

                session.add(new_dataset_obj)
                session.commit()
                session.refresh(new_dataset_obj)
                datasets_ids_new.append(new_dataset_obj.dataset_id)

            else: # If the dataset already exists, update it or ignore it.
                # Compare fields to decide whether to update or ignore.
                changed = False # We don't know yet if the dataset has changed info.

                # Compare and maybe update simple fields
                fields_to_check = [
                    "doi",
                    "date_created",
                    "date_last_modified",
                    "date_last_crawled",
                    "file_number",
                    # "download_number",
                    # "view_number",
                    # "license",
                    "url_in_data_source",
                    "title",
                    "description",
                    "keywords",
                ]
                changed = update_dataset_fields(existing_dataset, row, fields_to_check)


                # Compare many-to-many relationships for keywords and authors
                # Keywords in the database
                # existing_keywords = {kw.entry for kw in existing_dataset.keyword}
                # # Keywords in the current row
                # new_keywords = {kw.entry for kw in keyword_entries}
                # if existing_keywords != new_keywords:
                #     existing_dataset.keyword = keyword_entries
                #     changed = True
                
                # Authors in the database
                existing_authors = {author.name for author in existing_dataset.author}
                # Authors in the current row
                new_authors = {author.name for author in authors}
                if existing_authors != new_authors:
                    existing_dataset.author = authors
                    changed = True


                if changed: # If changed == True, update the dataset
                    session.add(existing_dataset)
                    session.commit()
                    datasets_ids_modified.append(existing_dataset.dataset_id)

                else:
                    datasets_ids_unchanged.append(existing_dataset.dataset_id)

    logger.success("Completed creating datasets tables.")
    logger.info(f"Entries created: {len(datasets_ids_new)}")
    logger.info(f"Entries updated: {len(datasets_ids_modified)}")
    logger.info(
        f"Entries ignored because they already exist in the database: {len(datasets_ids_unchanged)}"
        )

    datasets_ids_new_or_modified = datasets_ids_new + datasets_ids_modified

    return datasets_ids_new_or_modified


# ============================================================================
# File, FileType
# ============================================================================

def create_files_tables(
        files_df: pd.DataFrame,
        engine: Engine,
        datasets_ids_new_or_modified: list[int],
        ) -> None:
    """Create the FileType and File records in the database.

    We need to handle the FileType, Dataset, and recursive File relationships.
    """
    total_rows = len(files_df)

    # Counters for the number of records created, updated, or ignored.
    files_created_count = 0
    files_ignored_count = 0

    # Create a dictionary to store files by their name (if they are zip files)
    parent_files_by_name = {} # key: (dataset_id, file_name), value: File file_id

    with Session(engine) as session:
        for _, row in tqdm(
            files_df.iterrows(),
            total=total_rows,
            desc="Processing rows",
            unit="row"
            ):

            # --- Check if the File record already exists ---
            dataset_id_in_data_source = row["dataset_id_in_data_source"]
            data_source = row["data_source"]
            dataset_stmt = select(Dataset).join(DataSource).where(
                Dataset.id_in_data_source == dataset_id_in_data_source,
                DataSource.name == data_source,
            )
            current_dataset = session.exec(dataset_stmt).first()
            if not current_dataset:
                    logger.debug(
                        f"Dataset with id_in_data_source {dataset_id_in_data_source}",
                        f" and origin {data_source} not found."
                        )
                    continue  # Skip if not found

            # Determine if file exists already based on dataset status.
            existing_file = True
            # If the dataset is new or has been modified,
            # then all files are new to the database
            if current_dataset.dataset_id in datasets_ids_new_or_modified:
                existing_file = False

            if not existing_file:
                # File does not exist: create a new record with everything associated

                # --- Handle FileType (one-to-many relationship) ---
                file_type_name = row["type"]
                dict_to_add_file_type = {
                    "comment": None,  # CURRENTLY we don't have a comment
                }
                type_obj = get_or_create_models_with_one_attribute(session, FileType, "name", file_type_name, dict_to_add_file_type)


                # --- Handle Recursive File (parent-child relationship) ---
                # We have a column "parent_zip_file_name".
                # For files that are from a zip file, use this to find
                # the parent file file_id.
                parent_zip_file_name = row.get("parent_zip_file_name", None)
                parent_zip_file_id = None  # default is None

                # If the file is from a zip file, we need to find the parent file file_id
                if row["is_from_zip_file"] and parent_zip_file_name:
                    # Construct a key that combines the dataset id and parent's file name.
                    # Takes the dataset_id of the child file and the parent zip file_name
                    key = (current_dataset.dataset_id, parent_zip_file_name)

                    # Option 1: Check if we have already found the parent file in the cache.
                    parent_zip_file_id = parent_files_by_name.get(key, None)

                    if not parent_zip_file_id:
                        logger.debug(
                            f"Parent file with dataset id {current_dataset.dataset_id}",
                            f" and file name {parent_zip_file_name} not found in cache.")
                        logger.debug("Searching in the database...")

                        # Option 2: Query the DB using both the file name and dataset id.
                        parent_statement = (
                            select(File)(
                            # Find the parent file by name
                            File.name == row["parent_zip_file_name"],
                            # Make sure it's in the same dataset_id as the child file
                            File.dataset_id == current_dataset.dataset_id
                            )
                        )
                        parent_obj = session.exec(parent_statement).first()
                        if parent_obj:
                            parent_zip_file_id = parent_obj.file_id
                            # Cache the found parent file for later use.
                            parent_files_by_name[key] = parent_obj
                        else:
                            logger.error(
                                f"Parent file '{parent_zip_file_name}' not found for child"
                                f"'{row['name']}' with dataset_id {current_dataset.dataset_id}."
                                )

                new_file_obj = File(
                    name=row["name"],
                    size_in_bytes=row["size_in_bytes"],
                    md5=row["md5"],
                    url=row["url"],
                    is_from_zip_file=row["is_from_zip_file"],
                    # use the integer id from the Dataset record
                    dataset_id=current_dataset.dataset_id,
                    # use the file type id from FileType record
                    file_type_id=type_obj.file_type_id,
                    parent_zip_file_id = parent_zip_file_id
                )

                session.add(new_file_obj)
                session.commit()
                session.refresh(new_file_obj) # Refresh because we need the file_id for the parent zip file
                files_created_count += 1

                # If this file is a parent file (i.e. not extracted from a zip),
                # then store it in the cache using its dataset_id and name.
                if not row["is_from_zip_file"] and type_obj.name == "zip":
                    key = (current_dataset.dataset_id, row["name"])
                    parent_files_by_name[key] = new_file_obj.file_id
                
            else:
                # File exists: we ignore it.
                files_ignored_count += 1
    
    logger.success("Completed creating files tables.")
    logger.info(f"Entries created: {files_created_count}")
    logger.info(
        f"Entries ignored because they already exist in the database: {files_ignored_count}"
        )

# ============================================================================
# TopologyFile, Parameter,File, TrajectoryFile
# Thermostat, Barostat, Integrator
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

            dataset_id_in_data_source = row["dataset_id_in_data_source"]
            data_source = row["data_source"]
            statement_dataset = select(Dataset).join(DataSource).where(
                Dataset.id_in_data_source == dataset_id_in_data_source,
                DataSource.name == data_source
                )
            dataset_obj = session.exec(statement_dataset).first()
            if not dataset_obj:
                logger.debug(
                    f"Dataset with id_in_data_source {dataset_id_in_data_source}"
                    f" and origin {data_source} not found."
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

            dataset_id_in_data_source = row["dataset_id_in_data_source"]
            data_source = row["data_source"]
            statement = select(Dataset).join(DataSource).where(
                Dataset.id_in_data_source == dataset_id_in_data_source,
                DataSource.name == data_source
                )
            dataset_obj = session.exec(statement).first()
            if not dataset_obj:
                logger.debug(
                    f"Dataset with id_in_data_source {dataset_id_in_data_source}"
                    f" and origin {data_source} not found."
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
                thermostat=row["thermostat"],
                barostat=row["barostat"],
                integrator=row["integrator"]
            )

            session.add(parameter_obj)
            session.commit()

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

            dataset_id_in_data_source = row["dataset_id_in_data_source"]
            data_source = row["data_source"]
            statement = select(Dataset).join(DataSource).where(
                Dataset.id_in_data_source == dataset_id_in_data_source,
                DataSource.name == data_source
                )
            dataset_obj = session.exec(statement).first()
            if not dataset_obj:
                logger.debug(
                    f"Dataset with id_in_data_source {dataset_id_in_data_source}"
                    f" and origin {data_source} not found."
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


def create_simulation_tables(
        engine: Engine,
        ) -> None:
    """Create the TopologyFile, ParameterFile, and TrajectoryFile records in the database."""

    mdp_path = "data/parquet_files/gromacs_mdp_files.parquet"
    gro_path = "data/parquet_files/gromacs_gro_files.parquet"
    xtc_path = "data/parquet_files/gromacs_xtc_files.parquet"

    topology_df = load_topology_data(gro_path)
    parameter_df = load_parameter_data(mdp_path)
    trajectory_df = load_trajectory_data(xtc_path)

    logger.info("Creating simulation tables...\n")
    
    # TrajectoryFile
    logger.info("Creating TrajectoryFile table...")
    create_trajectory_table(trajectory_df, engine)
    logger.success("Completed creating trajectory table.\n")
    
    # ParameterFile, Thermostat, Barostat, Integrator
    logger.info("Creating ParameterFile, Thermostat, Barostat, Integrator tables...")
    create_parameters_table(parameter_df, engine)
    logger.success("Completed creating parameters tables.\n")
    
    # TopologyFile
    logger.info("Creating TopologyFile table...")
    create_topology_table(topology_df, engine)
    logger.success("Completed creating topology table.\n")

# ============================================================================

def data_ingestion():
    """
    Ingest data from parquet files into the database.

    For the simplicity of understanding, we will bundle up certain tables under
    the same function and "name":
    - The "datasets tables" will include:
        - Dataset
        - Author
        - DataSource
        - Keyword
    - The "files tables" will include:
        - File
        - FileType
    - The "simulation tables" will include:
        - TopologyFile
        - ParameterFile
        - TrajectoryFilen
        - Thermostat
        - Barostat
        - Integrator

    This function will ingest all the data in the following steps:
    1) Populate the "datsets tables".
        - Load the datasets data from the parquet file.
        - Create or update the tables.
        - Log the number of records created, updated, or ignored for Dataset.
    2) Populate the "files" tables.
        - Load the files data from the parquet file.
        - Check the datasets that have been updated or added.
            a) There are no new or modified datasets:
                - Skip the "files" ingestion.
            b) There are new or modified datasets:
                - Delete the files associated to the modified datasets.
                - Delete all the TopologyFile, ParameterFile, and TrajectoryFile tables.
                - Create the files tables.
                - Create the simulation tables.
    3) Log the total data ingestion time.
    """
    # Path to the parquet data files
    files_path = "data/parquet_files/files.parquet"
    datasets_path = "data/parquet_files/datasets.parquet"


    start_1 = time.perf_counter()
    start_4 = time.perf_counter()

    # Load the datasets data
    datasets_df = load_datasets_data(datasets_path)

    logger.info("Creating or updating datasets tables...")
    logger.info("Dataset, Author, DataSource, Keyword tables")
    new_or_modified_datasets = create_or_update_datasets_authors_origins_tables(datasets_df, engine)

    execution_time_1 = time.perf_counter() - start_1
    elapsed_time_1 = str(timedelta(seconds=execution_time_1)).split('.')[0]
    logger.info(f"Datasets ingestion time: {elapsed_time_1}\n")


    if len(new_or_modified_datasets) > 0:

        start_2 = time.perf_counter()

        # Load the files data
        files_df = load_files_data(files_path)
        logger.info("New or modified datasets found.")
        logger.info("Deleting files associated to modified datasets...")
        delete_files_for_update(engine, new_or_modified_datasets)

        logger.info("Creating files tables...")
        logger.info("File, FileType tables")
        create_files_tables(files_df, engine, new_or_modified_datasets)

        execution_time_2 = time.perf_counter() - start_2
        elapsed_time__2 = str(timedelta(seconds=execution_time_2)).split('.')[0]
        logger.info(f"Files ingestion time: {elapsed_time__2}\n")

        # Simulation data ingestion
        start_3 = time.perf_counter()

        create_simulation_tables(engine)

        execution_time_3 = time.perf_counter() - start_3
        elapsed_time_3 = str(timedelta(seconds=execution_time_3)).split('.')[0]
        logger.info(f"Simulation files ingestion time: {elapsed_time_3}\n")
    else:
        logger.info("No new or modified datasets found. Skipping files ingestion...\n")

    
    # Measure the total execution time
    execution_time_4 = time.perf_counter() - start_4
    elapsed_time_4 = str(timedelta(seconds=execution_time_4)).split('.')[0]

    logger.info(f"Data ingestion time: {elapsed_time_4}")
    logger.success("Data ingestion complete.")

if __name__ == "__main__":
    data_ingestion()
