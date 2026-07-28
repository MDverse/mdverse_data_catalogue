"""Scrape metadata of molecular dynamics datasets and files from ATLAS."""

import json
import re
import sys
from pathlib import Path

import click
import httpx
import loguru
from bs4 import BeautifulSoup

from mdverse.core.logger import create_logger
from mdverse.models.dataset import DatasetMetadata
from mdverse.models.enums import DatasetSourceName, ExternalDatabaseName, MoleculeType
from mdverse.models.file import FileMetadata
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
    get_last_modified_date_from_http_head_request,
    get_zip_file_content_from_http_request,
    is_connection_to_server_working,
    make_http_request_with_retries,
)
from .toolbox import print_statistics

BASE_URL = "https://www.dsimb.inserm.fr/ATLAS/"
BASE_API_URL = "https://www.dsimb.inserm.fr/ATLAS/api"
PDB_LIST_URL = "https://www.dsimb.inserm.fr/ATLAS/data/download/distributions/2024_11_18_ATLAS_pdb.txt"
AFFILIATION = (
    "Université Paris Cité and Université des Antilles and Université de la Réunion, "
    "INSERM, BIGR, F-75014 Paris, France"
)
ATLAS_METADATA = {
    "license": "CC-BY-NC",  # https://www.dsimb.inserm.fr/ATLAS/download.html
    "authors": [  # https://academic.oup.com/nar/article/52/D1/D384/7438909
        Person(
            full_name="Yann Vander Meersche",
            first_name="Yann",
            last_name="Vander Meersche",
            orcid="0000-0002-6680-8104",
            affiliation=AFFILIATION,
        ),
        Person(
            full_name="Gabriel Cretin",
            first_name="Gabriel",
            last_name="Cretin",
            orcid="0000-0002-3522-7083",
            affiliation=AFFILIATION,
        ),
        Person(
            full_name="Aria Gheeraert",
            first_name="Aria",
            last_name="Gheeraert",
            orcid="0000-0001-7493-8702",
            affiliation=AFFILIATION,
        ),
        Person(
            full_name="Jean-Christophe Gelly",
            first_name="Jean-Christophe",
            last_name="Gelly",
            orcid="0000-0001-5138-361X",
            affiliation=AFFILIATION,
        ),
        Person(
            full_name="Tatiana Galochkina",
            first_name="Tatiana",
            last_name="Galochkina",
            orcid="0000-0002-3608-5208",
            affiliation=AFFILIATION,
        ),
    ],
    "doi": "10.1093/nar/gkad1084",  # https://academic.oup.com/nar/article/52/D1/D384/7438909
    "external_link": ["https://www.dsimb.inserm.fr/ATLAS/"],
    "software_name": "GROMACS",  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
    "software_version": "v2019.6",  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
    "forcefield_name": "CHARMM36m",  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
    "forcefield_version": "July 2020",  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
    "water_model": "TIP3P",  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
    "simulation_temperature": 300,  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
    "simulation_time": "100 ns",  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
    "simulation_timestep": 2,  # https://www.dsimb.inserm.fr/ATLAS/api/MD_parameters
}


def extract_files_from_html(
    html: str, logger: "loguru.Logger" = loguru.logger
) -> list[dict]:
    """Extract file sizes from ATLAS dataset HTML page.

    Parameters
    ----------
    html : str
        HTML content of the ATLAS dataset page.
    logger : loguru.Logger
        Logger for logging messages.

    Returns
    -------
    list[dict]
        List of file names, sizes and urls found.

    """
    files_metadata = []
    download_link_pattern = re.compile(
        r"https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/[A-Za-z0-9]{4}_[A-Za-z]/.*zip"
    )
    soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        # Get links to zip files.
        match_link = download_link_pattern.search(href)
        if match_link:
            # Get file size.
            size = get_file_size_from_http_head_request(
                create_httpx_client(), href, logger=logger
            )
            files_metadata.append(
                {
                    "file_name": Path(href).name,
                    "file_url_in_repository": href,
                    "file_size_in_bytes": size,
                }
            )
    logger.info(f"Found {len(files_metadata)} files.")
    return files_metadata


def generate_description(
    metadata: SimulationMetadata, logger: "loguru.Logger" = loguru.logger
) -> str:
    """Generate description for ATLAS dataset based on metadata.

    Parameters
    ----------
    metadata : SimulationMetadata
        Metadata object for the dataset.
    logger : loguru.Logger
        Logger for logging messages.

    Returns
    -------
    str
        Generated description string.
    """
    description = "[automatically generated] Molecular dynamics simulation of "
    for molecule in metadata.molecules:
        description += f"{molecule.name} "
        description += f"from organism {molecule.organism} "
        external_identifiers = [
            f"{ext_id.database_name.value} ID: {ext_id.identifier}"
            for ext_id in molecule.external_identifiers
        ]
        if external_identifiers:
            description += "(" + ", ".join(external_identifiers) + ") "
    description += f"with {metadata.software[0].name} {metadata.software[0].version} "
    forcefields_models = [
        f"{ffm.name} {ffm.version}" if ffm.version else f"{ffm.name}"
        for ffm in metadata.forcefields_models
    ]
    if metadata.forcefields_models:
        description += "using " + " and ".join(forcefields_models) + " "
    description += f"at {metadata.simulation_temperatures_in_kelvin[0]} K "
    description += f"for {metadata.simulation_times[0]}."
    logger.debug("Dataset generated description:")
    logger.debug(description)
    return description


def scrape_metadata_for_one_dataset(
    client: httpx.Client,
    chain_id: str,
    logger: "loguru.Logger" = loguru.logger,
) -> dict | None:
    """Fetch metadata for a single ATLAS dataset (PDB chain).

    Parameters
    ----------
    client : httpx.Client
        HTTPX client for making requests.
    chain_id : str
        PDB chain identifier.
    logger : loguru.Logger
        Logger for logging messages.

    Returns
    -------
    dict | None
        Scraped dataset metadata, or None if failed.
    """
    logger.info(f"Scraping metadata for dataset: {chain_id}")
    api_url = f"{BASE_API_URL}/ATLAS/metadata/{chain_id}"
    dataset_url = (
        f"https://www.dsimb.inserm.fr/ATLAS/database/ATLAS/{chain_id}/{chain_id}.html"
    )
    response = make_http_request_with_retries(
        client, api_url, HttpMethod.GET, delay_before_request=0.5, logger=logger
    )
    if not response:
        logger.warning(f"Failed to fetch API data for {chain_id}. Skipping.")
        return None
    meta_json = None
    try:
        meta_json = response.json().get(f"{chain_id}")
    except (json.decoder.JSONDecodeError, KeyError) as exc:
        logger.warning("Failed to decode JSON response from the ATLAS API.")
        logger.warning(f"Error: {exc}")
        return None
    metadata = {
        "dataset_repository_name": DatasetSourceName.ATLAS,
        "dataset_id_in_repository": chain_id,
        "dataset_url_in_repository": dataset_url,
        "title": meta_json.get("protein_name"),
        "license": ATLAS_METADATA["license"],
        "authors": ATLAS_METADATA["authors"],
        "doi": ATLAS_METADATA["doi"],
        "external_links": ATLAS_METADATA["external_link"],
    }
    # Gather metadata for molecules.
    external_identifiers = []
    if meta_json.get("PDB"):
        external_identifiers.append(
            ExternalIdentifier(
                database_name=ExternalDatabaseName.PDB,
                identifier=meta_json["PDB"].split("_", maxsplit=1)[0],
            )
        )
    if meta_json.get("UniProt"):
        external_identifiers.append(
            ExternalIdentifier(
                database_name=ExternalDatabaseName.UNIPROT,
                identifier=meta_json["UniProt"],
            )
        )
    molecule = Molecule(
        name=meta_json.get("protein_name"),
        sequence=meta_json.get("sequence"),
        organism=meta_json.get("organism"),
        external_identifiers=external_identifiers,
        type=MoleculeType.PROTEIN,
    )
    # Gather metadata for software.
    software = [
        Software(
            name=ATLAS_METADATA["software_name"],
            version=ATLAS_METADATA["software_version"],
        )
    ]
    # Gather metadata for forcefields and models.
    forcefields_models = [
        ForceFieldModel(
            name=ATLAS_METADATA["forcefield_name"],
            version=ATLAS_METADATA["forcefield_version"],
        ),
        ForceFieldModel(name=ATLAS_METADATA["water_model"]),
    ]
    # Create simulation metadata.
    simulation_metadata = SimulationMetadata(
        molecules=[molecule],
        software=software,
        forcefields_models=forcefields_models,
        simulation_temperatures_in_kelvin=[ATLAS_METADATA["simulation_temperature"]],
        simulation_times=[ATLAS_METADATA["simulation_time"]],
        simulation_timesteps_in_fs=[ATLAS_METADATA["simulation_timestep"]],
    )
    metadata["simulation"] = simulation_metadata
    metadata["description"] = generate_description(simulation_metadata)
    logger.info("Done.")
    return metadata


def search_all_datasets(client: httpx.Client, logger: "loguru.Logger") -> set[str]:
    """Search for ATLAS datasets (1 dataset = 1 PDB chain).

    Parameters
    ----------
    client : httpx.Client
        HTTPX client for making requests.
    logger : loguru.Logger
        Logger for logging messages.

    Returns
    -------
    set[str]
        Set of PDB chains (datasets) found.
    """
    logger.info("Listing available datasets ...")
    response = make_http_request_with_retries(
        client, PDB_LIST_URL, HttpMethod.GET, delay_before_request=0.5, logger=logger
    )
    if not response or not hasattr(response, "text") or not response.text:
        logger.critical("Failed to fetch index page.")
        logger.critical("Cannot determine number of datasets. Aborting!")
        sys.exit(1)
    chain_ids = response.text.strip().splitlines()
    logger.info(f"Found {len(chain_ids)} datasets.")
    return chain_ids


def scrape_all_datasets(
    client: httpx.Client,
    pdb_chains: set[str],
    logger: "loguru.Logger",
) -> list[dict]:
    """Scrape all ATLAS datasets given a set of PDB chains.

    Parameters
    ----------
    pdb_chains : set[str]
        Set of PDB chains to scrape.
    logger : loguru.Logger
        Logger for logging messages.

    Returns
    -------
    list[dict]
        List of scraped dataset metadata.
    """
    datasets_meta = []
    logger.info("Starting scraping of all datasets...")
    for pdb_counter, pdb_chain in enumerate(pdb_chains, start=1):
        metadata = scrape_metadata_for_one_dataset(client, pdb_chain, logger=logger)
        if metadata:
            datasets_meta.append(metadata)
        logger.info(
            f"Scraped {pdb_counter:,}/{len(pdb_chains):,} "
            f"({pdb_counter / len(pdb_chains):.0%}) datasets"
        )
    return datasets_meta


def scrape_all_files(
    client: httpx.Client,
    datasets_metadata: list[DatasetMetadata],
    logger: "loguru.Logger",
) -> list[dict]:
    """Scrape ATLAS files.

    Parameters
    ----------
    datasets_metadata : list[DatasetMetadata]
        List of datasets metadata.
    logger : loguru.Logger
        Logger for logging messages.

    Returns
    -------
    list[dict]
        List of scraped files metadata.
    """
    files_metadata = []
    for dataset_counter, dataset_meta in enumerate(datasets_metadata, start=1):
        pdb_chain = dataset_meta.dataset_id_in_repository
        logger.info(f"Scraping files metadata for dataset: {pdb_chain}")
        url = dataset_meta.dataset_url_in_repository
        response = make_http_request_with_retries(
            client, url, HttpMethod.GET, delay_before_request=0.5, logger=logger
        )
        if not response:
            logger.warning(f"Failed to fetch HTML page for {pdb_chain}. Skipping.")
            continue
        files_meta = extract_files_from_html(response.text, logger=logger)
        for meta in files_meta:
            metadata = {
                "dataset_repository_name": dataset_meta.dataset_repository_name,
                "dataset_id_in_repository": dataset_meta.dataset_id_in_repository,
                "dataset_url_in_repository": dataset_meta.dataset_url_in_repository,
                "file_name": meta["file_name"],
                "file_url_in_repository": meta["file_url_in_repository"],
                "file_size_in_bytes": meta["file_size_in_bytes"],
            }
            files_metadata.append(metadata)
        # Select zip files.
        zip_files = [meta for meta in files_meta if meta["file_name"].endswith(".zip")]
        # Add content of the ZIP file.
        for zip_file in zip_files:
            zip_url = zip_file["file_url_in_repository"]
            zip_content = get_zip_file_content_from_http_request(zip_url, logger=logger)
            for file_item in zip_content:
                metadata = {
                    "dataset_repository_name": dataset_meta.dataset_repository_name,
                    "dataset_id_in_repository": dataset_meta.dataset_id_in_repository,
                    "dataset_url_in_repository": dataset_meta.dataset_url_in_repository,
                    "file_url_in_repository": zip_file["file_url_in_repository"],
                    "containing_archive_file_name": zip_file["file_name"],
                    "file_name": file_item["file_name"],
                    "file_size_in_bytes": file_item["file_size"],
                }
                files_metadata.append(metadata)
        logger.info(
            "Scraped metadata files for "
            f"{dataset_counter:,}/{len(datasets_metadata):,} "
            f"({dataset_counter / len(datasets_metadata):.0%}) datasets"
        )
        logger.info(f"Total files scraped so far: {len(files_metadata):,}")
    return files_metadata


def update_datasets_dates_from_files_metadata(
    client: httpx.Client,
    datasets_metadata: list[DatasetMetadata],
    files_metadata: list[FileMetadata],
    logger: "loguru.Logger",
) -> list[DatasetMetadata]:
    """Update datasets metadata based on files metadata.

    Update the date_created and date_last_modified fields of each dataset
    based on the last modified dates of its zip files.

    Parameters
    ----------
    datasets_metadata : list[DatasetMetadata]
        List of datasets metadata.
    files_metadata : list[FileMetadata]
        List of files metadata.
    logger : loguru.Logger
        Logger for logging messages.

    Returns
    -------
    list[DatasetMetadata]
        Updated list of datasets metadata.
    """
    updated_datasets_metadata = []
    logger.info("Updating datasets dates from files metadata...")
    for dataset_meta in datasets_metadata:
        logger.info(
            f"Updating metadata for dataset: {dataset_meta.dataset_id_in_repository}"
        )
        dates = []
        number_of_files = 0
        for file_meta in files_metadata:
            if (
                file_meta.dataset_id_in_repository
                == dataset_meta.dataset_id_in_repository
                and file_meta.file_type == "zip"
            ):
                last_modified = get_last_modified_date_from_http_head_request(
                    client, file_meta.file_url_in_repository, logger=logger
                )
                dates.append(last_modified)
            if (
                file_meta.dataset_id_in_repository
                == dataset_meta.dataset_id_in_repository
            ):
                number_of_files += 1
        if dates:
            dataset_meta.date_created = min(dates)
            dataset_meta.date_last_updated = max(dates)
        if number_of_files > 0:
            dataset_meta.number_of_files = number_of_files
        logger.info(f"Date created: {dataset_meta.date_created}")
        logger.info(f"Date last updated: {dataset_meta.date_last_updated}")
        logger.info(f"Number of files: {dataset_meta.number_of_files}")
        updated_datasets_metadata.append(dataset_meta)
        logger.info(
            "Updated metadata for "
            f"{len(updated_datasets_metadata):,}/{len(datasets_metadata):,} "
            f"({len(updated_datasets_metadata) / len(datasets_metadata):.0%}) datasets"
        )
    return updated_datasets_metadata


@click.command(
    help="Command line interface for MDverse scrapers",
    epilog="Happy scraping!",
)
@click.option(
    "--output-dir",
    "output_dir_path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
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
    """Scrape metadata of molecular dynamics datasets and files from ATLAS."""
    # Create scraper context.
    scraper = ScraperContext(
        data_source_name=DatasetSourceName.ATLAS,
        output_dir_path=output_dir_path,
        is_in_debug_mode=is_in_debug_mode,
    )
    # Create logger.
    level = "INFO"
    if scraper.is_in_debug_mode:
        level = "DEBUG"
    logger = create_logger(logpath=scraper.log_file_path, level=level)
    # Print scraper configuration.
    logger.debug(scraper.model_dump_json(indent=4, exclude={"token"}))
    logger.info("Starting ATLAS data scraping...")
    # Create HTTPX client
    client = create_httpx_client()
    # Check connection to the ATLAS API
    if is_connection_to_server_working(
        client, f"{BASE_API_URL}/ATLAS/metadata/16pk_A", logger=logger
    ):
        logger.success("Connection to ATLAS API successful!")
    else:
        logger.critical("Connection to ATLAS API failed.")
        logger.critical("Aborting.")
        sys.exit(1)
    # Scrape datasets metadata.
    datasets_ids = search_all_datasets(client=client, logger=logger)
    if scraper.is_in_debug_mode:
        datasets_ids = set(list(datasets_ids)[:10])
        logger.warning("Debug mode is ON: limiting to first 10 datasets.")
    datasets_metadata = scrape_all_datasets(
        client,
        datasets_ids,
        logger=logger,
    )
    # Normalize datasets metadata.
    datasets_metadata_normalized = normalize_datasets_metadata(
        datasets_metadata,
        logger=logger,
    )
    # Scrape files metadata.
    files_metadata = scrape_all_files(
        client,
        datasets_metadata_normalized,
        logger=logger,
    )
    # Normalize datasets metadata.
    files_metadata_normalized = normalize_files_metadata(
        files_metadata,
        logger=logger,
    )
    # Update date_created and date_last_modified fields in datasets
    # based on zip files modification dates.
    datasets_metadata_normalized = update_datasets_dates_from_files_metadata(
        client, datasets_metadata_normalized, files_metadata_normalized, logger=logger
    )
    # Save datasets metadata to parquet file.
    scraper.number_of_datasets_scraped = export_list_of_models_to_parquet(
        scraper.datasets_parquet_file_path,
        datasets_metadata_normalized,
        logger=logger,
    )
    # Save files metadata to parquet file.
    scraper.number_of_files_scraped = export_list_of_models_to_parquet(
        scraper.files_parquet_file_path,
        files_metadata_normalized,
        logger=logger,
    )
    # Print scraping statistics.
    print_statistics(scraper, logger=logger)


if __name__ == "__main__":
    main()
