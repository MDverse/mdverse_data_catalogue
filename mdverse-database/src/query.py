import random
import time

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import aliased
from sqlmodel import Session, select

from db_schema import engine
from db_schema import Dataset, DataSource, File, FileType

"""Purpose
This script demonstrates how to query the database to extract and print
information.

We measure the execution time of the script in order to get an idea of
how long it takes to complete a query on the database.

To launch this script, use the command:
uv run python src/query.py
"""

start = time.perf_counter()


def print_data_source_summary():
    """
    Prints a summary of the dataset data source. For each data source,
    it prints the number of datasets, the first dataset creation date,
    and the last dataset creation date.

    This is used mainly to demonstrate that the data was loaded correctly.
    The results are printed to the console and compared to the original data
    that can be found on https://mdverse.streamlit.app/
    """

    with Session(engine) as session:
        # Build a query that joins DataSource and Dataset,
        # groups by data source name, and aggregates the data.
        statement = (
            select(
                DataSource.name.label("dataset_data_source"),
                func.count(Dataset.dataset_id).label("number_of_datasets"),
                func.min(Dataset.date_created).label("first_dataset"),
                func.max(Dataset.date_created).label("last_dataset"),
            )
            # We join on the relationship: dataset_origin.origin_id == dataset.origin_id
            .join(Dataset, Dataset.data_source_id == DataSource.data_source_id)
            .group_by(DataSource.name)
        )

        results = session.exec(statement).all()

        # Print header
        header = (
            f"{'Dataset_origin':<15}{'Number of datasets':<20}"
            f"{'First_dataset':<15}{'Last_dataset':<15}"
        )
        print(header)
        print("-" * len(header))

        # Print each origin row.
        total_count = 0
        for row in results:
            total_count += row.number_of_datasets

            first_date = row.first_dataset if row.first_dataset else "None"
            last_date = row.last_dataset if row.last_dataset else "None"
            print(
                f"{row.dataset_data_source:<15}{row.number_of_datasets:<20}"
                f"{first_date:<15}{last_date:<15}"
            )

        # Add a totals row.
        print("-" * len(header))
        print(f"{'total':<15}{total_count:<20}{'None':<15}{'None':<15}\n")


def query_to_dataframe():
    with Session(engine) as session:
        # Build a statement that selects Dataset records,
        # joins to DatasetOrigin on the origin_id,
        # and filters to only those with DatasetOrigin.name == "zenodo"
        statement = (
            select(Dataset)
            .join(DataSource, Dataset.data_source_id == DataSource.data_source_id)
            .where(DataSource.name == "zenodo")
        )
        results = session.exec(statement).all()  # This returns SQLModel objects

        # Convert the SQLModel objects into dictionaries so that pandas can
        # work with them.
        data = [dataset.model_dump() for dataset in results]
        df = pd.DataFrame(data)

        # Print some information about the extracted dataframe
        print(df.head(), "\n")
        print(df.dtypes, "\n")
        print(df.columns, "\n")

def random_mdp_information():

    with Session(engine) as session:
        # When you perform a join on the same table more than once—in this case,
        # joining the File table to itself—you need a way to distinguish between
        # the two instances. This is where an alias comes in.
        ParentFile = aliased(File) # Alias for the parent file  # noqa: N806

        # Build a query that:
        # - Joins File to its Dataset and DatasetOrigin, and FileType
        # - Does a left outer join to ParentFile so we can get the parent's name.
        # - Filters for files whose name ends with ".mdp" (case-insensitive).
        statement = (
            select(
                Dataset.id_in_data_source,
                Dataset.url_in_data_source,
                DataSource.name.label("dataset_origin"),
                File.name.label("file_name"),
                File.size_in_bytes,
                FileType.name.label("file_type"),
                ParentFile.name.label("parent_file_name")
            )
            .join(Dataset, File.dataset_id == Dataset.dataset_id)
            .join(DataSource, Dataset.data_source_id == DataSource.data_source_id)
            .join(FileType, File.file_type_id == FileType.file_type_id)
            .outerjoin(ParentFile, File.parent_zip_file_id == ParentFile.file_id)
            .where(FileType.name == "mdp")
        )

        # Execute the query and retrieve all matching rows
        results = session.exec(statement).all()
        count_mdp = len(results)
        print(f"Total number of .mdp files: {count_mdp}\n")

        if count_mdp == 0:
            print("No .mdp file found.")
            return

        # Select one random file
        random_row = random.choice(results)

        # Print the information about the random file
        # Prepare a dictionary to represent the table row.
        # (If a file is not from a zip file, parent_file_name will be None.)
        table_row = {
            "Dataset.id_in_origin": random_row.id_in_data_source,
            "Dataset.origin": random_row.dataset_origin,
            "Dataset.url": random_row.url_in_data_source,
            "File.name": random_row.file_name,
            "File.size_in_bytes": random_row.size_in_bytes,
            # Using the parent's name here instead of the ID
            "File.parent_zip_file_id": random_row.parent_file_name
        }

        # Print the result in a vertical (row-by-row) format.
        print("Random .mdp file Information:")
        print("-" * 40)
        for key, value in table_row.items():
            # Convert None to a string "None" for safe formatting
            value_str = value
            print(f"{key}: {value_str}")
        print("-" * 40, "\n")

def print_datasets_no_files():

    with Session(engine) as session:
        # Build a query that from joining Dataset and File tables:
        # - Selects all datasets that have no files.
        # - Groups by Dataset.
        # - Filters for datasets with no files.
        statement = (
            select(Dataset)
            .outerjoin(File, Dataset.dataset_id == File.dataset_id)
            .group_by(Dataset)
            .having(func.count(File.file_id) == 0)
        )
        results = session.exec(statement).all()

        # Print the number of datasets with no files.
        print(f"Number of datasets with no files: {len(results)}\n")

        # If there are datasets with no files, print the first 5 dataset IDs.
        if results:
            # Print all the datasets with no files.
            print("Dataset IDs with no files:")
            for dataset in results:
                print(f"Dataset ID in database: {dataset.dataset_id}\n Datset ID in origin: {dataset.id_in_data_source} \n Origin: {dataset.id_in_data_source}\n")
            print("\n")

def main():
    print_data_source_summary()
    print("\n\n")
    query_to_dataframe()
    print("\n\n")
    random_mdp_information()
    print("\n\n")
    print_datasets_no_files()
    print("\n\n")

if __name__ == "__main__":
    main()

execution_time = time.perf_counter() - start
print(f"Query execution time: {execution_time:.2f} seconds")
