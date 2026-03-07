from pathlib import Path

import requests

"""Purpose:
This script demonstrates how to download data from Zenodo using the Zenodo API.
It fetches the metadata of a Zenodo record and downloads all .parquet files
associated with that record. The files are saved to a specified folder (if the
folder does not exist, it is created).
"""

# Zenodo API URL
ZENODO_RECORD_ID = "7856806"
ZENODO_API_URL = f"https://zenodo.org/api/records/{ZENODO_RECORD_ID}"

# Set the output folder
OUTPUT_FOLDER = "data/parquet_files"

# Ensure the output folder exists (create it if necessary)
output_path = Path(OUTPUT_FOLDER)
if not output_path.exists():
    output_path.mkdir(parents=True, exist_ok=True)
    print(f"Created folder: {OUTPUT_FOLDER}\n")


def get_parquet_files():
    """
    Fetches and downloads all .parquet files
    from Zenodo to a specified folder.
    """

    # Step 1: Fetch the JSON metadata
    response = requests.get(ZENODO_API_URL)
    response.raise_for_status()  # Raise an error if request fails
    data = response.json()

    # Step 2: Extract all .parquet file URLs and filenames
    for file_entry in data.get("files", []):
        filename = file_entry["key"]
        if filename.endswith(".parquet"):  # Filter for .parquet files
            file_url = file_entry["links"]["self"]

            # Define full file path
            file_path = output_path / filename

            # Step 3: Download and save each file
            print(f"Downloading {filename} to {OUTPUT_FOLDER}...\n")
            file_response = requests.get(file_url)
            with open(file_path, "wb") as f:
                f.write(file_response.content)
            print(f"Saved: {file_path}")


# Call the function
get_parquet_files()
