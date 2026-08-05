"""Scraper for molecular dynamics papers via Europe PMC and Hugging Face."""

import contextlib
import os
import re
import time
from collections.abc import Generator
from datetime import timedelta
from pathlib import Path

import click
import httpx
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pydantic import ValidationError

from mdverse.core.logger import create_logger
from mdverse.models.ai_model import AiModelCoreMetadata
from mdverse.models.dataset import DatasetCoreMetadata
from mdverse.models.enums import DatasetSourceName, PublicationSourceName
from mdverse.models.person import Person
from mdverse.models.publication import PublicationMetadata
from mdverse.scrapers.network import (
    create_httpx_client,
    is_connection_to_server_working,
)

EUROPE_PMC_PING_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=test&format=json&pageSize=1"
EUROPE_PMC_SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
HF_PING_URL = "https://huggingface.co/api/papers/search?q=test&limit=1"
HF_BASE_URL = "https://huggingface.co"
HF_PAPERS_SEARCH_QUERY = "molecular dynamics"
EUROPE_PMC_SEARCH_QUERY = (
    '((TITLE:("molecular dynamics" AND (simulation* OR trajector* OR dataset*))) '
    "AND OPEN_ACCESS:y AND HAS_FT:y NOT TITLE:review* "
    'NOT PUB_TYPE:"Review" NOT PUB_TYPE:"review-article")'
)
METHODS_KEYWORDS = [
    "method",
    "materials and methods",
    "experimental procedures",
    "simulation details",
    "computational methods",
    "methodology",
]
REPOSITORY_URL_PATTERN = re.compile(
    r"https?://(?:www\.)?"
    r"(?:"
    r"github\.com/[a-zA-Z0-9\-_]+/[a-zA-Z0-9\-_.]+|"  # GitHub repos
    r"zenodo\.org/(?:record|records)/[0-9]+|"  # Zenodo records
    r"doi\.org/10\.5281/zenodo\.[0-9]+|"  # Zenodo DOIs
    r"[a-zA-Z0-9\-]+\.figshare\.com/[^\s><'\"()]+|"  # Figshare subdomains
    r"figshare\.com/articles/[^\s><'\"()]+|"  # Figshare articles
    r"doi\.org/10\.6084/[^\s><'\"()]+|"  # Figshare DOIs
    r"datadryad\.org/[^\s><'\"()]+|"  # Dryad
    r"osf\.io/[a-zA-Z0-9]+|"  # Open Science Framework
    r"huggingface\.co/[a-zA-Z0-9\-_]+/[a-zA-Z0-9\-_.]+"  # Hugging Face
    r")",
    re.IGNORECASE,
)
REPO_DOMAIN_MAP = {
    "zenodo": DatasetSourceName.ZENODO,
    "figshare": DatasetSourceName.FIGSHARE,
    "10.6084": DatasetSourceName.FIGSHARE,
    "osf.io": DatasetSourceName.OSF,
    "huggingface.co": DatasetSourceName.HUGGINGFACE,
}


def load_existing_publications_from_parquet(
    parquet_path: Path,
) -> tuple[pd.DataFrame, set[str], dict[str, int]]:
    """Load existing dataset from a Parquet file to avoid duplicates.

    Returns
    -------
        tuple[pd.DataFrame, set[str], dict[str, int]]: DataFrame, DOIs, and counts.
    """
    if parquet_path.exists():
        # Load the dataframe and convert DOIs to lowercase for consistent comparison.
        dataframe = pd.read_parquet(parquet_path)
        dois = set(dataframe["doi"].dropna().str.lower())
        counts = {}
        # Calculate the distribution of existing publications across known sources.
        if "publication_source_name" in dataframe.columns:
            source_counts = dataframe["publication_source_name"].value_counts()
            counts = {
                enum_value.value: int(source_counts.get(enum_value.value, 0))
                for enum_value in PublicationSourceName
            }
        return dataframe, dois, counts
    # Return empty structures when no previous parquet file is found.
    return (
        pd.DataFrame(),
        set(),
        {enum_value.value: 0 for enum_value in PublicationSourceName},
    )


def safe_get(
    client: httpx.Client, url: str, params: dict | None = None, max_retries: int = 3
) -> httpx.Response:
    """Execute an HTTP GET request with exponential backoff for rate limits.

    Returns
    -------
        httpx.Response: The HTTP response object.
    """
    for attempt in range(max_retries):
        # Fire the HTTP request with the provided parameters.
        response = client.get(url, params=params)
        # Handle API rate limiting
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            # Calculate wait time using exponential backoff
            # or the Retry-After header if provided.
            wait_time = (
                int(retry_after)
                if retry_after and retry_after.isdigit()
                else 2 ** (attempt + 1)
            )
            time.sleep(wait_time)
            continue
        return response
    return response


def _extract_header_info(line: str, next_line: str | None) -> tuple[str | None, int]:
    """Extract header title and markdown depth level from a text line.

    Returns
    -------
        tuple[str | None, int]: Lowercase header title and depth level (1-6).
    """
    stripped = line.strip()
    # Normalize inline ATX headers by isolating the hash symbols.
    if "#" in stripped and not stripped.startswith("#"):
        stripped = stripped[stripped.find("#") :]
    # Match standard Markdown headers and calculate their depth level.
    match = re.match(r"^#{1,6}\s+", stripped)
    if match:
        return stripped.lstrip("#").strip().lower(), len(match.group().strip())
    # Identify bold text patterns often used as fallback sub-headers.
    if re.match(r"^\*\*\d*(\.\d+)*\s*.*?\*\*", stripped):
        return stripped.strip("*").strip().lower(), 2
    # Detect Setext headers characterized by underlining on the following line.
    if next_line:
        next_string = next_line.strip()
        if next_string and set(next_string) in ({"="}, {"-"}):
            return stripped.lower(), 1 if "=" in next_string else 2
    return None, 0


def _extract_methods_fallback(lines: list[str]) -> str | None:
    """Extract methods content using keyword matching as a fallback.

    Returns
    -------
        str | None: Extracted Methods content or None.
    """
    methods_lines, is_in_section = [], False
    for line in lines:
        stripped = line.strip()
        lower_line = stripped.lower()
        # Toggle section state if a line strongly matches methodology keywords.
        if (
            not is_in_section
            and any(keyword in lower_line for keyword in METHODS_KEYWORDS)
            and len(stripped) < 60
        ):
            is_in_section = True
        # Break the extraction loop when encountering distinct paper sections.
        elif (
            is_in_section
            and any(
                lower_line.startswith(stop_word)
                for stop_word in ["results", "discussion", "references"]
            )
            and len(stripped) < 40
        ):
            break
        # Append lines to the results once inside the recognized section.
        if is_in_section:
            methods_lines.append(line)
    return "\n".join(methods_lines).strip() or None


def _extract_methods_structured(lines: list[str]) -> str | None:
    """Extract methods content strictly based on Markdown headers.

    Returns
    -------
        str | None: Extracted Methods text or None if not found.
    """
    methods_lines, in_section, skip_next, section_level = [], False, False, 0
    for index, line in enumerate(lines):
        # Skip the iteration if the line was already processed as a Setext underline.
        if skip_next:
            skip_next = False
            continue
        next_line = lines[index + 1] if index + 1 < len(lines) else None
        header, level = _extract_header_info(line, next_line)
        if header:
            clean_header = re.sub(r"^\d+(\.\d+)*\.?\s*", "", header).strip()
            # Capture the methods section when the header explicitly matches.
            if any(keyword in clean_header for keyword in METHODS_KEYWORDS):
                in_section, section_level = True, level
                methods_lines.append(line)
                if next_line and set(next_line.strip()) in ({"-"}, {"="}):
                    methods_lines.append(next_line)
                    skip_next = True
                continue
            # Terminate extraction if an equivalent/higher-level header is encountered.
            if in_section and level <= section_level:
                break
        if in_section:
            methods_lines.append(line)
    return "\n".join(methods_lines).strip() or None


def extract_materials_and_methods_hf(
    client: httpx.Client, paper_id: str, logger: object
) -> str | None:
    """Fetch and extract the methodology section for a Hugging Face paper.

    Returns
    -------
        str | None: Extracted Methods section text or None.
    """
    url = f"https://huggingface.co/papers/{paper_id}.md"
    # Attempt to retrieve the raw Markdown representation of the paper.
    try:
        response = safe_get(client, url)
        if response.status_code != 200:
            logger.error(
                f"[HF] Markdown fetch failed for {paper_id} ({response.status_code})."
            )
            return None
    except httpx.HTTPError as error:
        logger.error(f"[HF] Extraction failed for {paper_id}: {error}.")
        return None
    # Process the text using the structured approach first, then the regex fallback.
    lines = response.text.splitlines()
    text = _extract_methods_structured(lines) or _extract_methods_fallback(lines)
    if not text:
        logger.warning(f"[HF] No Methods section found in {paper_id}.")
    return text


def search_hf_papers(
    client: httpx.Client, limit: int | None, logger: object
) -> list[dict]:
    """Search the Hugging Face papers API using predefined keywords.

    Returns
    -------
        list[dict]: List of paper records.
    """
    # Forward the query to the dedicated Hugging Face endpoint and collect results.
    response = safe_get(
        client,
        f"{HF_BASE_URL}/api/papers/search",
        params={"q": HF_PAPERS_SEARCH_QUERY, "limit": limit or 120},
    )
    if response.status_code != 200:
        logger.error(f"[HF] Search failed with HTTP {response.status_code}.")
        return []
    return response.json()


def parse_authors(authors_data: list[dict]) -> list[Person]:
    """Parse raw author dictionaries into structured Person instances.

    Returns
    -------
        list[Person]: Validated Person models.
    """
    persons = []
    # Split the full name strings to guess first and last names for each author.
    for author in authors_data:
        full_name = author.get("name")
        if full_name:
            parts = full_name.strip().split()
            persons.append(
                Person(
                    first_name=parts[0] if parts else None,
                    last_name=" ".join(parts[1:]) if len(parts) > 1 else None,
                    full_name=full_name,
                )
            )
    return persons


def parse_hf_record(
    client: httpx.Client, raw_item: dict, logger: object
) -> PublicationMetadata | None:
    """Parse a raw Hugging Face paper record into a publication model.

    Returns
    -------
        PublicationMetadata | None: Validated paper model or None.
    """
    # Verify the existence of fundamental metadata fields before further processing.
    paper_id = raw_item.get("id") or raw_item.get("paper", {}).get("id")
    title = raw_item.get("title") or raw_item.get("paper", {}).get("title")
    if not paper_id or not title:
        return None
    # Enrich the base item with detailed information from the dedicated paper API.
    time.sleep(0.5)
    response = safe_get(client, f"{HF_BASE_URL}/api/papers/{paper_id}")
    paper_details = (
        raw_item | response.json()
        if response and response.status_code == 200
        else raw_item
    )
    linked_datasets, linked_models = [], []
    # Convert linked Hugging Face datasets into DatasetCoreMetadata.
    for item in paper_details.get("linkedDatasets", []):
        dataset_id = item.get("id")
        if dataset_id:
            with contextlib.suppress(ValidationError):
                linked_datasets.append(
                    DatasetCoreMetadata(
                        dataset_repository_name=DatasetSourceName.HUGGINGFACE,
                        dataset_id_in_repository=str(dataset_id),
                        dataset_url_in_repository=f"https://huggingface.co/datasets/{dataset_id}",
                    )
                )
    # Convert linked Hugging Face AI models into AiModelCoreMetadata.
    for item in paper_details.get("linkedModels", []):
        model_id = item.get("id")
        if model_id:
            with contextlib.suppress(ValidationError):
                linked_models.append(
                    AiModelCoreMetadata(
                        repository_name=DatasetSourceName.HUGGINGFACE,
                        model_id_in_repository=str(model_id),
                        model_url=f"https://huggingface.co/{model_id}",
                    )
                )
    # Assemble the final Pydantic publication object.
    try:
        return PublicationMetadata(
            doi=f"10.48550/arxiv.{paper_id}".lower(),
            publication_source_name=PublicationSourceName.HUGGINGFACE,
            publication_id_in_source=str(paper_id),
            url=f"https://huggingface.co/papers/{paper_id}",
            title=title,
            year=str(paper_details.get("publishedAt", ""))[:4],
            keywords=paper_details.get("ai_keywords", []),
            authors=parse_authors(paper_details.get("authors", [])),
            abstract=paper_details.get("summary"),
            journal="Hugging Face Daily Papers",
            materials_and_methods=extract_materials_and_methods_hf(
                client, str(paper_id), logger
            ),
            dataset_references=linked_datasets,
            model_references=linked_models,
        )
    except ValidationError as error:
        logger.error(f"[HF] Schema error ({paper_id}): {error}.")
        return None


def search_europe_pmc_stream(
    client: httpx.Client, max_results: int | None
) -> Generator[list[dict]]:
    """Stream search results continuously via Europe PMC cursor pagination.

    Yields
    ------
        list[dict]: Page chunk of metadata records.
    """
    cursor, total = "*", 0
    # Execute paginated requests continuously until the cursor signifies the last page.
    while True:
        response = client.get(
            EUROPE_PMC_SEARCH_URL,
            params={
                "query": EUROPE_PMC_SEARCH_QUERY,
                "format": "json",
                "pageSize": 50,
                "resultType": "core",
                "cursorMark": cursor,
            },
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()
        results = data.get("resultList", {}).get("result", [])
        if not results:
            break
        yield results
        # Update counters and evaluate the breaking condition for maximum limits.
        total += len(results)
        next_cursor = data.get("nextCursorMark")
        if (max_results and total >= max_results) or (next_cursor == cursor):
            break
        cursor = next_cursor


def _extract_methods_and_links_pmc(
    client: httpx.Client, pmcid: str, logger: object
) -> tuple[str | None, list[str]]:
    """Retrieve XML full text to extract specific methods tags and links.

    Returns
    -------
        tuple[str | None, list[str]]: Methods text and external links.
    """
    url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
    # Initiate connection to download the full XML document.
    try:
        response = client.get(url, timeout=30.0)
        if response.status_code != 200:
            logger.error(
                f"[PMC] Fetch XML failed for {pmcid} ({response.status_code})."
            )
            return None, []
    except httpx.HTTPError as error:
        logger.error(f"[PMC] XML request failed for {pmcid}: {error}.")
        return None, []
    # Identify unique URLs matching defined repository formats within the raw text.
    soup = BeautifulSoup(response.content, "xml")
    found_links = list(
        {
            match.rstrip(".,;()[]")
            for match in REPOSITORY_URL_PATTERN.findall(soup.get_text())
        }
    )
    # Isolate paragraph texts contained strictly within valid methodology section tags.
    kw_pattern = re.compile("|".join(METHODS_KEYWORDS), re.IGNORECASE)
    methods = [
        "\n".join(paragraph.get_text(strip=True) for paragraph in section.find_all("p"))
        for section in soup.find_all("sec")
        if kw_pattern.search(f"{section.get('sec-type', '')} {section.find('title')}")
    ]
    methods_text = "\n\n".join(filter(None, methods)).strip() or None
    if not methods_text:
        logger.warning(f"[PMC] No Methods section found in {pmcid}.")
    return methods_text, found_links


def _categorize_links(links: list[str]) -> tuple[list[DatasetCoreMetadata], list[str]]:
    """Separate explicit data repositories from standard external links.

    Returns
    -------
        tuple[list[DatasetCoreMetadata], list[str]]: Datasets and external links.
    """
    dataset_refs, ext_links = [], []
    for link in links:
        lower_link = link.lower()
        # Check if the extracted link belongs to any explicitly tracked database enum.
        matching_source = next(
            (
                enum_val
                for domain_string, enum_val in REPO_DOMAIN_MAP.items()
                if domain_string in lower_link
            ),
            None,
        )
        if matching_source:
            with contextlib.suppress(ValidationError):
                dataset_refs.append(
                    DatasetCoreMetadata(
                        dataset_repository_name=matching_source,
                        dataset_id_in_repository=link.rstrip("/").split("/")[-1],
                        dataset_url_in_repository=link,
                    )
                )
        # Keep non-recognized URLs as standard external references.
        else:
            ext_links.append(link)
    return dataset_refs, ext_links


def parse_pmc_record(
    client: httpx.Client, raw_item: dict, logger: object
) -> PublicationMetadata | None:
    """Parse a raw Europe PMC record into a publication model.

    Returns
    -------
        PublicationMetadata | None: Validated paper record or None.
    """
    doi_val = raw_item.get("doi")
    title_val = raw_item.get("title")
    pmcid = raw_item.get("pmcid")
    # Abort parsing if any mandatory bibliographic identifier is missing.
    if not doi_val or not title_val or not pmcid:
        return None
    clean_doi = str(doi_val).strip().lower()
    # Trigger XML evaluation to split methodology sections and links.
    methods_text, all_links = _extract_methods_and_links_pmc(client, str(pmcid), logger)
    # Separate recognized datasets and generic external references.
    dataset_refs, ext_links = _categorize_links(all_links)
    # Parse author metadata.
    authors_data = raw_item.get("authorList", {}).get("author", []) or []
    parsed_authors = [
        Person(
            first_name=author.get("firstName"),
            last_name=author.get("lastName"),
            full_name=author.get("fullName"),
            orcid=next(
                (
                    auth_id.get("value")
                    for auth_id in [author.get("authorId") or {}]
                    if auth_id.get("type") == "ORCID"
                ),
                None,
            ),
            affiliation=next(
                (
                    auth_aff.get("affiliation")
                    for auth_aff in author.get("authorAffiliationDetailsList", {}).get(
                        "authorAffiliation", []
                    )
                    or []
                ),
                None,
            ),
        )
        for author in authors_data
    ]
    # Instantiate the Pydantic PublicationMetadata model.
    try:
        return PublicationMetadata(
            doi=clean_doi,
            publication_source_name=PublicationSourceName.EUROPE_PMC,
            publication_id_in_source=str(pmcid),
            url=f"https://doi.org/{clean_doi}",
            title=title_val,
            authors=parsed_authors,
            year=str(raw_item.get("pubYear", "")),
            abstract=raw_item.get("abstractText"),
            journal=raw_item.get("journalInfo", {}).get("journal", {}).get("title"),
            keywords=raw_item.get("keywordList", {}).get("keyword", []),
            materials_and_methods=methods_text,
            external_links=ext_links,
            dataset_references=dataset_refs,
        )
    except ValidationError as error:
        logger.error(f"[PMC] Schema error ({clean_doi}): {error}.")
        return None


def export_papers_to_parquet(
    dataframe: pd.DataFrame,
    new_papers: list[PublicationMetadata],
    filepath: Path,
    logger: object,
) -> pd.DataFrame:
    """Export combined scraped publication records to a Parquet file.

    Returns
    -------
        pd.DataFrame: Updated DataFrame with combined records.
    """
    # Transform Python models to a dataframe and merge them.
    new_df = pd.DataFrame([paper.model_dump() for paper in new_papers])
    combined = (
        pd.concat([dataframe, new_df], ignore_index=True).drop_duplicates(
            subset=["doi"], keep="first"
        )
        if not dataframe.empty
        else new_df
    )
    # Save to parquet, ensuring the directory exists.
    filepath.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(filepath, index=False)
    logger.success(f"Batch saved to {filepath.name}.")
    return combined


def scrape_europe_pmc_source(
    client: httpx.Client,
    existing_df: pd.DataFrame,
    existing_dois: set[str],
    output_path: Path,
    nb_test: int | None,
    batch_size: int,
    pending_batch: list[PublicationMetadata],
    logger: object,
) -> tuple[pd.DataFrame, list[PublicationMetadata], int]:
    """Iterate over Europe PMC stream pages and save papers in batches.

    Returns
    -------
    tuple[pd.DataFrame, list[PublicationMetadata], int]:
        Updated dataframe, pending batch, scraped count.
    """
    pmc_count = 0
    # Walk through the endless results generator and parse items individually.
    for page_results in search_europe_pmc_stream(client, max_results=None):
        for raw_item in page_results:
            if nb_test and pmc_count >= nb_test:
                return existing_df, pending_batch, pmc_count
            doi_val = raw_item.get("doi")
            if not doi_val or str(doi_val).strip().lower() in existing_dois:
                continue
            paper = parse_pmc_record(client, raw_item, logger)
            # Register validated papers into the pending queue for delayed saving.
            if paper:
                pmc_count += 1
                existing_dois.add(paper.doi)
                pending_batch.append(paper)
                logger.info(f"[PMC] Scraped #{pmc_count} ({paper.doi})")
                if len(pending_batch) >= batch_size:
                    existing_df = export_papers_to_parquet(
                        existing_df, pending_batch, output_path, logger
                    )
                    pending_batch = []
    return existing_df, pending_batch, pmc_count


def scrape_huggingface_source(
    client: httpx.Client,
    existing_df: pd.DataFrame,
    existing_dois: set[str],
    output_path: Path,
    nb_test: int | None,
    batch_size: int,
    pending_batch: list[PublicationMetadata],
    logger: object,
) -> tuple[pd.DataFrame, list[PublicationMetadata], int]:
    """Fetch results from Hugging Face and process them incrementally.

    Returns
    -------
    tuple[pd.DataFrame, list[PublicationMetadata], int]:
        Updated dataframe, pending batch, scraped count.
    """
    hf_count = 0
    # Interrogate Hugging Face for matching publications and filter duplicates.
    for raw_item in search_hf_papers(client, limit=nb_test, logger=logger):
        if nb_test and hf_count >= nb_test:
            break
        paper_id = raw_item.get("id") or raw_item.get("paper", {}).get("id")
        if not paper_id:
            continue
        arxiv_doi = f"10.48550/arxiv.{paper_id}".lower()
        if arxiv_doi in existing_dois:
            continue
        paper = parse_hf_record(client, raw_item, logger)
        # Offload processed models in groups dictated by the batch size threshold.
        if paper:
            hf_count += 1
            existing_dois.add(paper.doi)
            pending_batch.append(paper)
            logger.info(f"[HF] Scraped #{hf_count} ({paper.doi})")
            if len(pending_batch) >= batch_size:
                existing_df = export_papers_to_parquet(
                    existing_df, pending_batch, output_path, logger
                )
                pending_batch = []
    return existing_df, pending_batch, hf_count


def _run_scraper_source(
    name: str,
    ping_url: str,
    scraper_func,
    client: httpx.Client,
    existing_df: pd.DataFrame,
    existing_dois: set[str],
    existing_counts: dict[str, int],
    output_path: Path,
    nb_test: int | None,
    batch_size: int,
    pending_batch: list[PublicationMetadata],
    logger: object,
) -> tuple[pd.DataFrame, list[PublicationMetadata], int]:
    """Evaluate server connection and dispatch the requested scraper function.

    Returns
    -------
    tuple[pd.DataFrame, list[PublicationMetadata], int]:
        Updated dataframe, pending batch, scraped count.
    """
    scraped_count = 0
    logger.info(f"=== Scraping {name} ===")
    # Secure server connection validation prior to initiating heavy routines.
    if is_connection_to_server_working(client, ping_url, logger=logger):
        logger.success(f"Connection to {name} API successful!")
        existing_df, pending_batch, scraped_count = scraper_func(
            client,
            existing_df,
            existing_dois,
            output_path,
            nb_test,
            batch_size,
            pending_batch,
            logger,
        )
    else:
        logger.warning(f"Connection failed. Skipping {name} source.")
    if scraped_count == 0:
        logger.warning(
            f"[{name}] No new papers scraped. All fetched are already in "
            f"{output_path.name} ({existing_counts.get(name, 0)} existing)."
        )
    return existing_df, pending_batch, scraped_count


@click.command(
    help="Scraper for MD papers via Europe PMC and Hugging Face.",
    epilog="Happy scraping!",
)
@click.option(
    "--output-path",
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
    required=True,
    help="Output file path to save the Parquet file.",
)
@click.option(
    "--nb-test",
    "nb_test",
    type=int,
    default=None,
    help="Max number of papers to scrape per source (for testing/debug).",
)
@click.option(
    "--batch-size",
    "batch_size",
    type=int,
    default=10,
    help="Number of scraped papers per batch saving.",
)
@click.option(
    "--hf",
    "use_hf",
    is_flag=True,
    default=False,
    help="Enable or disable Hugging Face scraping source.",
)
@click.option(
    "--pmc",
    "use_pmc",
    is_flag=True,
    default=False,
    help="Enable or disable Europe PMC scraping source.",
)
def main(
    output_path: Path,
    nb_test: int | None,
    batch_size: int,
    *,
    use_hf: bool,
    use_pmc: bool,
) -> None:
    """Scrape paper metadata from both Europe PMC and Hugging Face APIs."""
    level = "DEBUG" if nb_test else "INFO"
    logger = create_logger(logpath="logs/scrap_publications.log", level=level)
    logger.info("Starting MDverse Paper Scraper.")
    start_time = time.perf_counter()
    # Verify if parquet file exists and load existing DOIs.
    existing_df, existing_dois, existing_counts = (
        load_existing_publications_from_parquet(output_path)
    )
    pmc_source_key = PublicationSourceName.EUROPE_PMC.value
    hf_source_key = PublicationSourceName.HUGGINGFACE.value
    logger.info(
        f"Loaded {len(existing_dois)} total existing papers from {output_path.name}."
    )
    # Create HTTPX client.
    load_dotenv()
    client = create_httpx_client()
    hf_token = os.getenv("HF_TOKEN")
    if hf_token:
        logger.info("Using Hugging Face API token.")
        client.headers["Authorization"] = f"Bearer {hf_token}"
    # Execute Source: Hugging Face Papers.
    pending_batch = []
    total_scraped = 0
    if use_hf:
        existing_df, pending_batch, count_hf = _run_scraper_source(
            PublicationSourceName.HUGGINGFACE.value,
            HF_PING_URL,
            scrape_huggingface_source,
            client,
            existing_df,
            existing_dois,
            existing_counts,
            output_path,
            nb_test,
            batch_size,
            pending_batch,
            logger,
        )
        total_scraped += count_hf
    # Execute Source: Europe PMC.
    if use_pmc:
        existing_df, pending_batch, count_pmc = _run_scraper_source(
            PublicationSourceName.EUROPE_PMC.value,
            EUROPE_PMC_PING_URL,
            scrape_europe_pmc_source,
            client,
            existing_df,
            existing_dois,
            existing_counts,
            output_path,
            nb_test,
            batch_size,
            pending_batch,
            logger,
        )
        total_scraped += count_pmc
    # Final batch save if any pending papers remain.
    if pending_batch:
        export_papers_to_parquet(
            existing_df, pending_batch, output_path, client, logger
        )
    elapsed = str(timedelta(seconds=time.perf_counter() - start_time)).split(".")[0]
    logger.info(f"Total new papers saved: {total_scraped} to: {output_path}.")
    logger.success(f"Successfully completed scraping in {elapsed}.")


if __name__ == "__main__":
    main()
