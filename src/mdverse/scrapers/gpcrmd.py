"""Scrape molecular dynamics simulation datasets and files from GPCRmd.

This script scrapes molecular dynamics datasets from the GPCRmd repository
https://www.gpcrmd.org/dynadb/search/
"""

import json
import re
import sys
from pathlib import Path
from typing import Any

import click
import httpx
import loguru
from bs4 import BeautifulSoup

from mdverse.core.logger import create_logger
from mdverse.models.enums import DatasetSourceName
from mdverse.models.person import Person
from mdverse.models.scraper import ScraperContext
from mdverse.models.simulation import (
    ExternalIdentifier,
    ForceFieldModel,
    Molecule,
    SimulationMetadata,
    Software,
)
from mdverse.models.utils import (
    export_list_of_models_to_parquet,
    normalize_datasets_metadata,
    normalize_files_metadata,
)

from .network import (
    HttpMethod,
    create_httpx_client,
    get_file_size_from_http_head_request,
    is_connection_to_server_working,
    make_http_request_with_retries,
)
from .toolbox import print_statistics

BASE_GPCRMD_URL = "https://www.gpcrmd.org/api/search_all"


def scrape_all_datasets(
    client: httpx.Client,
    url: str,
    scraper: ScraperContext,
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Scrape Molecular Dynamics-related datasets from the GPCRmd API.

    Returns
    -------
    list[dict]:
        A list of GPCRmd entries.
    """
    logger.info("Scraping molecular dynamics datasets from GPCRmd.")
    all_datasets = []
    response = make_http_request_with_retries(
        client,
        url,
        method=HttpMethod.GET,
        timeout=60,
        delay_before_request=0.2,
    )
    if not response:
        logger.critical("Failed to fetch data from GPCRmd API.")
        sys.exit(1)
    else:
        try:
            # Get the formatted response with request metadata in JSON format
            all_datasets = response.json()
        except (json.decoder.JSONDecodeError, ValueError) as exc:
            logger.error(f"Error while parsing GPCRmd response: {exc}")
            logger.error("Cannot find datasets.")
            logger.critical("Aborting.")
            sys.exit(1)
    if not all_datasets:
        logger.critical("No datasets found in GPCRmd.")
        logger.critical("Aborting.")
        sys.exit(1)
    else:
        # Log the first dataset raw metadata.
        logger.debug("First dataset raw metadata:")
        logger.debug(all_datasets[0])
        logger.success(f"Scraped {len(all_datasets)} datasets in GPCRmd.")

    if scraper and scraper.is_in_debug_mode and len(all_datasets) >= 10:
        logger.warning("Debug mode is ON: stopping after 10 datasets.")
        # Return only the first 10 datasets for testing purposes.
        return all_datasets[:10]
    return all_datasets


def fetch_all_datasets_html_pages(
    client: httpx.Client, datasets: list[dict], logger: "loguru.Logger" = loguru.logger
) -> list[list[str] | None]:
    """Fetch all datasets HTML pages.

    Returns
    -------
    list[list[str] | None]
        The HTML content of datasets pages.
    """
    logger.info("Fetching HTML content for all datasets.")
    datasets_html_pages = []

    for dataset_counter, dataset in enumerate(datasets, start=1):
        # Get the URL of the current dataset
        dataset_id = str(dataset.get("dyn_id"))
        logger.info(f"Scraping dataset ID `{dataset_id}`.")
        url = dataset.get("url")
        page_content = None
        # If the dataset has a URL, attempt to fetch its HTML content
        if url:
            response = make_http_request_with_retries(
                client,
                url,
                method=HttpMethod.GET,
                timeout=60,
                delay_before_request=0.2,
            )
            # If the request was successful
            if response:
                # Store the HTML text
                html_content = response.text
                logger.debug(f"HTML length: {len(html_content)} characters")
                # Extract HTML page content.
                soup = BeautifulSoup(html_content, "html.parser")
                # Preserve link targets
                link_targets = [
                    link.get("href", "") for link in soup.find_all("a", href=True)
                ]
                # Split the full text into separate lines for line-by-line processing
                page_content = []
                for line in soup.get_text().splitlines():
                    stripped_line = line.strip()
                    if stripped_line:
                        page_content.append(stripped_line)
                page_content.extend(link_targets)
            else:
                logger.warning(
                    f"Failed to fetch HTML page for dataset ID `{dataset_id}`."
                )
        datasets_html_pages.append(page_content)
        logger.success(
            f"Scraped {dataset_counter:,}/{len(datasets):,} "
            f"datasets ({dataset_counter / len(datasets):.0%})."
        )
    return datasets_html_pages


def _extract_molecules_from_lines(lines: list[str]) -> list[Molecule] | None:
    """
    Extract a list of molecules from text lines.

    The function looks for a "Number of molecules" section and parses
    subsequent lines expected to follow the format "Name: count".
    Parsing stops as soon as the format no longer matches this pattern.

    Returns
    -------
    list[Molecule] | None
        A list of Molecule objects if at least one molecule is found,
        otherwise None.
    """
    molecules: list[Molecule] = []
    capture = False

    for line in lines:
        clean_line = line.strip()
        # Skip empty lines
        if not clean_line:
            continue
        # Start capturing after the "Number of molecules" header
        if "Number of molecules" in clean_line:
            capture = True
            continue
        if capture:
            # Stop when the expected "Name: number" pattern is no longer found.
            if ":" not in clean_line or "Total" in clean_line:
                break
            name, count = clean_line.split(":", 1)
            # Stop if the count is not a valid integer.
            if not count.strip().isdigit():
                break
            molecules.append(
                Molecule(
                    name=name.strip(),
                    number_of_molecules=int(count.strip()),
                )
            )

    return molecules or None


def retrieve_metadata_from_html_dataset_page(
    html_content: list[str] | None,
    field_name: str | None,
    dataset_id: str,
    logger: "loguru.Logger" = loguru.logger,
) -> list[str] | list[Molecule]:
    """
    Retrieve a specific metadata field from a webpage.

    Returns
    -------
    list[str] | list[Molecule]
        The value of the metadata field in a list if found, otherwise an empty list.

    """
    if not html_content or not field_name:
        return []
    try:
        # Special case for molecules and their number of atoms
        if field_name == "Number of molecules":
            return _extract_molecules_from_lines(html_content)

        for line in html_content:
            if field_name not in line:
                continue

            # Special case for DOI.
            if field_name == "doi" and "doi:" in line:
                doi = line.split("doi:", 1)[1].strip()
                # Usually the doi is at the end of a sentence.
                if doi and doi.endswith("."):
                    # So we remove the period.
                    doi = doi[:-1]
                return [f"https://doi.org/{doi}"]

            # General case.
            separator = f"{field_name}:"
            if separator in line:
                # Return the text after the separator, stripped
                return [line.split(separator, 1)[1].strip()]

    except (AttributeError, TypeError, ValueError) as exc:
        logger.warning(
            f"Error parsing field '{field_name}' for dataset {dataset_id}: {exc}"
        )

    return []


def scrape_files_metadata_for_one_dataset(
    client: httpx.Client,
    html_content: list[str] | None,
    core_metadata: dict[str, Any],
    logger: "loguru.Logger" = loguru.logger,
) -> list[dict]:
    """
    Scrape files metadata for a given dataset.

    Returns
    -------
    list[dict]
        List of files metadata dictionaries.
    """
    files_metadata = []
    # Extract metadata from dataset url page if available.
    if not html_content:
        logger.error("Failed to fetch files metadata.")
        return files_metadata

    # Find all <a> tags with href containing the files path.
    # Example of files found for dataset ID `17``:
    # Dataset URL: https://www.gpcrmd.org/dynadb/dynamics/id/7/
    # /dynadb/files/Dynamics/10166_trj_7.dcd
    # /dynadb/files/Dynamics/10167_dyn_7.psf
    # /dynadb/files/Dynamics/10168_dyn_7.pdb
    for line in html_content:
        if "/dynadb/files/Dynamics/" not in line:
            continue
        # Add core metadata.
        metadata = core_metadata.copy()
        # Add URL and file name.
        metadata["file_url_in_repository"] = f"https://www.gpcrmd.org{line.strip()}"
        metadata["file_name"] = Path(metadata["file_url_in_repository"]).name
        # Fetch the file size using a HEAD request.
        # We do not download the entire file.
        metadata["file_size_in_bytes"] = get_file_size_from_http_head_request(
            client, metadata["file_url_in_repository"], logger=logger
        )
        files_metadata.append(metadata)
    logger.info(f"Total files found: {len(files_metadata)}.")
    return files_metadata


def _parse_authors(raw_authors: str | None) -> list[Person]:
    """Parse raw author string into a list of Person models.

    Returns
    -------
    list[Person]
        A list of Person objects representing the authors.
    """
    if not raw_authors or not raw_authors.strip():
        return []
    raw_str = raw_authors.strip()

    # Handle comma case: left = authors, right = affiliation
    if "," in raw_str:
        names_part, _, aff_part = raw_str.partition(",")
        affiliation = aff_part.strip() or None
        words = names_part.strip().split()
        n_words = len(words)
        # Multiple authors case: >= 4 words and even count (pairs of first/last names)
        if n_words >= 4 and n_words % 2 == 0:
            return [
                Person(
                    first_name=words[i],
                    last_name=words[i + 1],
                    full_name=f"{words[i]} {words[i + 1]}",
                    affiliation=affiliation,
                )
                for i in range(0, n_words, 2)
            ]
        # Single author case
        elif words:
            first = words[0] if n_words > 1 else None
            last = " ".join(words[1:]) if n_words > 1 else words[0]
            return [
                Person(
                    first_name=first,
                    last_name=last,
                    full_name=names_part.strip(),
                    affiliation=affiliation,
                )
            ]

    # No comma: handle parenthesized affiliation exception (e.g. "Group Name (Univ)")
    if match := re.search(r"^(.*?)\s*\((.*?)\)$", raw_str):
        return [
            Person(
                full_name=match.group(1).strip(),
                affiliation=match.group(2).strip() or None,
            )
        ]
    # No comma, no parentheses: fallback to plain full_name only
    return [Person(full_name=raw_str)]


def extract_datasets_and_files_metadata(
    client: httpx.Client,
    datasets: list[dict[str, Any]],
    datasets_html_content: list[list[str] | None],
    logger: "loguru.Logger" = loguru.logger,
) -> tuple[list[dict], list[dict]]:
    """
    Extract relevant metadata from raw GPCRmd datasets metadata.

    Returns
    -------
    list[dict]
        List of dataset metadata dictionaries.
    list[dict]
        List of file metadata dictionaries.
    """
    datasets_metadata = []
    files_metadata = []
    for dataset, html_content in zip(datasets, datasets_html_content, strict=True):
        dataset_id = str(dataset.get("dyn_id"))
        logger.info(f"Extracting metadata for dataset ID `{dataset_id}`.")
        dataset_url = dataset.get("url")
        # Add core metadata for the dataset.
        core_metadata = {
            "dataset_repository_name": DatasetSourceName.GPCRMD,
            "dataset_id_in_repository": dataset_id,
            "dataset_url_in_repository": dataset_url,
        }
        # Initialize dataset metadata.
        dataset_dict = {
            **core_metadata,
            "title": dataset.get("modelname"),
            "date_created": dataset.get("creation_timestamp"),
        }
        # Extract other metadata from dataset url page if available.
        if html_content is None:
            logger.warning(f"Cannot parse HTML metadata for dataset `{dataset_id}`.")
            logger.warning(dataset_url)
            logger.warning("Skipping this step.")
            datasets_metadata.append(dataset_dict)
            continue
        # Description.
        dataset_dict["description"] = retrieve_metadata_from_html_dataset_page(
            html_content=html_content, field_name="Description", dataset_id=dataset_id
        )[0]
        # Reference links.
        dataset_dict["external_links"] = retrieve_metadata_from_html_dataset_page(
            html_content=html_content, field_name="doi", dataset_id=dataset_id
        )
        # Author names.
        raw_authors = retrieve_metadata_from_html_dataset_page(
            html_content=html_content, field_name="Submitted by", dataset_id=dataset_id
        )[0]
        dataset_dict["authors"] = _parse_authors(raw_authors)
        # Retrieve the files metadata from the html content of the dataset page.
        files_metadata_for_this_dataset = scrape_files_metadata_for_one_dataset(
            client, html_content, core_metadata, logger=logger
        )
        files_metadata.extend(files_metadata_for_this_dataset)
        dataset_dict["number_of_files"] = len(files_metadata_for_this_dataset)
        # Extract simulation metadata from the API if available.
        # Software names with their versions.
        if dataset.get("mysoftware"):
            software = [
                Software(
                    name=dataset["mysoftware"], version=dataset.get("software_version")
                )
            ]
        # Forcefields and models.
        forcefields_and_models = []
        if dataset.get("forcefield"):
            forcefields_and_models.append(
                ForceFieldModel(
                    name=dataset["forcefield"],
                    version=dataset.get("forcefield_version"),
                )
            )
        # Get solvent model from the HTML content of the dataset page.
        solvent_model = retrieve_metadata_from_html_dataset_page(
            html_content=html_content, field_name="Solvent type", dataset_id=dataset_id
        )
        if solvent_model:
            forcefields_and_models.append(
                ForceFieldModel(name=solvent_model[0], version=None)
            )
        # Molecules.
        # Get Uniprot ID and PDB ID from the API metadata.
        db_links = []
        uniprot_id = dataset.get("uniprot")
        if uniprot_id:
            db_links.append(
                ExternalIdentifier(
                    database_name="uniprot",
                    identifier=uniprot_id,
                    url=f"https://www.uniprot.org/uniprotkb/{uniprot_id}/entry",
                )
            )
        pdb_id = dataset.get("pdb_namechain")
        if pdb_id:
            db_links.append(
                ExternalIdentifier(
                    database_name="pdb",
                    identifier=pdb_id,
                    url=f"https://www.rcsb.org/structure/{pdb_id}",
                )
            )
        molecules = [
            Molecule(
                name=dataset.get("protname"),
                organism=dataset.get("species"),
                external_identifiers=db_links,
            )
        ]
        # Add other molecules scraped from HTML.
        # Like membrane lipids, ions, water, etc.
        html_mols = retrieve_metadata_from_html_dataset_page(
            html_content, "Number of molecules", dataset_id
        )
        if html_mols:
            molecules.extend(html_mols)
        # Simulation time.
        simulation_times = retrieve_metadata_from_html_dataset_page(
            html_content=html_content,
            field_name="Accumulated simulation time",
            dataset_id=dataset_id,
        )
        # Convert the timestep string (e.g., "4.0 fs")
        # to a float representing the number of femtoseconds
        timestep = dataset.get("timestep")
        if not isinstance(timestep, float) and timestep is not None:
            timestep = float(dataset.get("timestep").split()[0])
        # Adding full metadata for the dataset.
        dataset_dict["simulation"] = SimulationMetadata(
            total_number_of_atoms=dataset.get("atom_num"),
            simulation_timesteps_in_fs=[timestep],
            software=software,
            forcefields_models=forcefields_and_models,
            molecules=molecules,
            simulation_times=simulation_times,
        )
        datasets_metadata.append(dataset_dict)
        logger.success(
            f"Scraped metadata for {len(datasets_metadata):,}/{len(datasets):,} "
            f"datasets ({len(datasets_metadata) / len(datasets):.0%})."
        )
    return datasets_metadata, files_metadata


@click.command(
    help="Command line interface for MDverse scrapers",
    epilog="Happy scraping!",
)
@click.option(
    "--output-dir",
    "output_dir_path",
    type=click.Path(exists=False, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Output directory path to save results.",
)
@click.option(
    "--debug",
    "is_in_debug_mode",
    is_flag=True,
    default=False,
    help="Enable debug mode.",
)
def main(output_dir_path: Path, *, is_in_debug_mode: bool = False) -> None:
    """Scrape molecular dynamics datasets and files from GPCRmd."""
    # Create scraper context.
    scraper = ScraperContext(
        data_source_name=DatasetSourceName.GPCRMD,
        output_dir_path=output_dir_path,
        is_in_debug_mode=is_in_debug_mode,
    )
    # Create logger.
    level = "DEBUG" if scraper.is_in_debug_mode else "INFO"
    logger = create_logger(logpath=scraper.log_file_path, level=level)
    # Print scraper configuration.
    logger.debug(scraper.model_dump_json(indent=4, exclude={"token"}))
    logger.info("Starting GPCRmd scraping...")
    # Create HTTPX client.
    client = create_httpx_client()
    # Check connection to GPCRmd API.
    if is_connection_to_server_working(
        client, f"{BASE_GPCRMD_URL}/pdbs/", logger=logger
    ):
        logger.success("Connection to GPCRmd API successful!")
    else:
        logger.critical("Connection to GPCRmd API failed.")
        logger.critical("Aborting.")
        sys.exit(1)
    # Scrape GPCRmd datasets metadata.
    datasets_raw_metadata = scrape_all_datasets(
        client=client,
        url=f"{BASE_GPCRMD_URL}/info/",
        scraper=scraper,
        logger=logger,
    )
    # Fetch the dataset HTML page for all datasets
    datasets_html_content = fetch_all_datasets_html_pages(
        client, datasets_raw_metadata, logger=logger
    )
    # Extract datasets and files metadata
    datasets_selected_metadata, files_metadata = extract_datasets_and_files_metadata(
        client, datasets_raw_metadata, datasets_html_content, logger=logger
    )
    # Validate GPCRmd datasets metadata with the DatasetMetadata Pydantic model.
    datasets_normalized_metadata = normalize_datasets_metadata(
        datasets_selected_metadata, logger=logger
    )
    # Save datasets metadata to parquet file.
    scraper.number_of_datasets_scraped = export_list_of_models_to_parquet(
        scraper.datasets_parquet_file_path,
        datasets_normalized_metadata,
        logger=logger,
    )
    # Validate GPCRmd files metadata with the FileMetadata Pydantic model.
    files_normalized_metadata = normalize_files_metadata(files_metadata, logger=logger)
    # Save files metadata to parquet file.
    scraper.number_of_files_scraped = export_list_of_models_to_parquet(
        scraper.files_parquet_file_path,
        files_normalized_metadata,
        logger=logger,
    )
    # Print scraping statistics.
    print_statistics(scraper, logger=logger)


if __name__ == "__main__":
    main()
