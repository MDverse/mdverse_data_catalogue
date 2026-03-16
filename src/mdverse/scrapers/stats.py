"""Get aggregate stats for scrapers."""

from pathlib import Path

import click
import pandas as pd


@click.command(
    help="Get statistics for scrapers.",
)
@click.option(
    "--input",
    "input_file_path_list",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path),
    required=True,
    multiple=True,
    help="Parquet file path to read data from.",
)
@click.option(
    "--debug",
    "is_in_debug_mode",
    is_flag=True,
    default=False,
    help="Enable debug mode.",
)
def main(input_file_path_list: list[Path], *, is_in_debug_mode: bool = False) -> None:
    """Get statistics for all Parquet files passed as arguments."""
    datasets_df = pd.DataFrame()
    files_df = pd.DataFrame()
    for parquet_file_path in input_file_path_list:
        print(f"Reading data from: {parquet_file_path}")
        tmp_df = pd.read_parquet(parquet_file_path)
        if "dataset" in parquet_file_path.stem:
            print(f"Adding {len(tmp_df):,} datasets")
            datasets_df = pd.concat([datasets_df, tmp_df], ignore_index=True)
        elif "file" in parquet_file_path.stem:
            print(f"Adding {len(tmp_df):,} files")
            files_df = pd.concat([files_df, tmp_df], ignore_index=True)
        else:
            print(
                f"Unknown category (dataset or file) for {parquet_file_path}. Skipping."
            )
            continue
    print(f"Total datasets: {len(datasets_df):,}")
    print(f"Total files: {len(files_df):,}")


if __name__ == "__main__":
    main()
