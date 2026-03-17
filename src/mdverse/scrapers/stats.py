"""Get aggregate stats for scrapers."""

from pathlib import Path

import click
import numpy as np
import pandas as pd


def list_parquet_files(dir_list: list[Path] | list[str]) -> list[Path]:
    """List all Parquet files in the given directories.

    Parameters
    ----------
    dir_list : list[Path] | list[str]
        List of directory paths to search for Parquet files.

    Returns
    -------
    list[Path]
        List of Parquet file paths found in the given directories.
    """
    parquet_files = []
    dir_path_list = [Path(dir_path) for dir_path in dir_list]
    for dir_path in dir_path_list:
        parquet_files.extend(dir_path.rglob("*.parquet"))
    return parquet_files


def read_datasets_files_dataframes(
    parquet_files: list[Path],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read Parquet files and return datasets and files DataFrames.

    Parameters
    ----------
    parquet_files : list[Path]
        List of Parquet file paths.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        Tuple containing datasets DataFrame and files DataFrame.
    """
    datasets_df = pd.DataFrame()
    files_df = pd.DataFrame()
    for parquet_file_path in parquet_files:
        print(f"Reading data from: {parquet_file_path}")
        tmp_df = pd.read_parquet(parquet_file_path)
        if "dataset" in parquet_file_path.name:
            print(f"Adding {len(tmp_df):,} datasets")
            datasets_df = pd.concat([datasets_df, tmp_df], ignore_index=True)
        elif "file" in parquet_file_path.name:
            print(f"Adding {len(tmp_df):,} files")
            files_df = pd.concat([files_df, tmp_df], ignore_index=True)
        else:
            print(
                f"Unknown category (dataset or file) for {parquet_file_path}. Skipping."
            )
            continue
    return datasets_df, files_df


def aggregate_datasets(datasets_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate datasets DataFrame by year and repository.

    Parameters
    ----------
    datasets_df : pd.DataFrame
        DataFrame containing dataset information.

    Returns
    -------
    pd.DataFrame
        Aggregated DataFrame with counts of datasets by year and repository.
    """
    datasets_df["date_created"] = pd.to_datetime(datasets_df["date_created"]).dt.date
    datasets_agg = datasets_df.groupby("dataset_repository_name").agg(
        number_of_datasets=("dataset_id_in_repository", "nunique"),
        date_first_dataset=("date_created", "min"),
        date_last_dataset=("date_created", "max"),
    )
    return datasets_agg


def aggregate_files(files_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate files DataFrame by year and repository.

    Parameters
    ----------
    files_df : pd.DataFrame
        DataFrame containing file information.

    Returns
    -------
    pd.DataFrame
        Aggregated DataFrame with counts of files by year and repository.
    """
    files_df["_is_zip_file"] = np.where((files_df["file_type"] == "zip"), True, False)  # noqa FBT003
    files_df["_size_not_from_zip_file"] = np.where(
        (files_df["containing_archive_file_name"].isna()),
        files_df["file_size_in_bytes"] / 1e9,
        0.0,
    )
    files_df["_is_not_from_zip_file"] = files_df["containing_archive_file_name"].isna()
    files_agg = files_df.groupby("dataset_repository_name").agg(
        number_of_datasets=("dataset_id_in_repository", "nunique"),
        deposited_files=("_is_not_from_zip_file", "sum"),
        deposited_zip_files=("_is_zip_file", "sum"),
        deposited_file_size_in_GB=("_size_not_from_zip_file", "sum"),
        total_number_of_files=("dataset_id_in_repository", "count"),
    )
    return files_agg


@click.command(
    help="Get statistics for scrapers.",
)
@click.option(
    "--dir",
    "dir_list",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    multiple=True,
    help="Directory path to read Parquet files from.",
)
@click.option(
    "--debug",
    "is_in_debug_mode",
    is_flag=True,
    default=False,
    help="Enable debug mode.",
)
def main(dir_list: list[Path], *, is_in_debug_mode: bool = False) -> None:
    """Get statistics for all Parquet files passed as arguments."""
    parquet_files = list_parquet_files(dir_list)
    print(f"Found {len(parquet_files)} Parquet files")
    if len(parquet_files) == 0:
        print("No Parquet files found. Exiting.")
        return
    datasets_df, files_df = read_datasets_files_dataframes(parquet_files)
    print(f"Total datasets: {len(datasets_df):,}")
    print(f"Total files: {len(files_df):,}")
    # datasets_agg = aggregate_datasets(datasets_df)
    # files_agg = aggregate_files(files_df)


if __name__ == "__main__":
    main()
