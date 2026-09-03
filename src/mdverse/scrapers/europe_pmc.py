"""Scraper for molecular dynamics papers via Europe PMC."""

import html
import re
import time
from collections.abc import Generator
from datetime import timedelta
from pathlib import Path

import click
import httpx
import loguru
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pydantic import ValidationError

from mdverse.core.logger import create_logger
from mdverse.models.enums import PublicationSourceName
from mdverse.models.person import Person
from mdverse.models.publication import PublicationMetadata
from mdverse.scrapers.network import (
    create_httpx_client,
    is_connection_to_server_working,
)

EUROPE_PMC_PING_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=test&format=json&pageSize=1"
EUROPE_PMC_SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
# Europe PMC query targeting Title, Abstract, and Methods sections
# - TITLE_ABS: matches keywords in article title or abstract
# - METHODS: searches strictly inside full-text methodology sections
# - IN_PMC:y: ensures full-text XML availability for parsing
# - OPEN_ACCESS:y: restricts retrieval to open-access articles
# - NOT: removes reviews, editorials, errata, and corrections
EUROPE_PMC_SEARCH_QUERY = (
    "("
    "("
    "TITLE_ABS:("
    '("molecular dynamics" OR "MD") AND '
    "(simulation* OR trajector* OR dataset*)"
    ") "
    "OR "
    "METHODS:("
    '("molecular dynamics" OR "MD") AND '
    "(simulation* OR trajector*)"
    ")"
    ") "
    "AND OPEN_ACCESS:y "
    "AND IN_PMC:y "
    "AND NOT TITLE:review* "
    'AND NOT PUB_TYPE:"Review" '
    'AND NOT PUB_TYPE:"review-article" '
    'AND NOT PUB_TYPE:"Editorial" '
    'AND NOT PUB_TYPE:"Erratum"'
    ")"
)


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


def parse_authors(authors_data: list[dict]) -> list[Person]:
    """Parse raw author dictionaries into structured Person instances.

    Returns
    -------
        list[Person]: Validated Person models.
    """
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
    return parsed_authors


def clean_markup_text(raw_text: str | None) -> str | None:
    """Strip XML/HTML markup tags, decode HTML entities, and normalize whitespace.

    Returns
    -------
    str | None
        Sanitized plain text, or None if the input is empty or invalid.
    """
    if not raw_text or not isinstance(raw_text, str):
        return None
    # Decode HTML entities (&amp;, &lt;, &#x003b;, etc.)
    decoded_text = html.unescape(raw_text)
    # Strip HTML/XML tags and keep text content
    plain_text = BeautifulSoup(decoded_text, "html.parser").get_text(separator=" ")
    # Collapse multiple spaces, non-breaking spaces (\xa0), and trailing whitespace
    cleaned_text = re.sub(r"\s+", " ", plain_text).strip()
    return cleaned_text or None


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


def fetch_pmc_xml(
    client: httpx.Client,
    pmcid: str,
    logger: "loguru.Logger" = loguru.logger,
) -> str | None:
    """Fetch XML content for a PMCID and parse it into a BeautifulSoup object.

    Returns
    -------
        str | None: The raw XML content, or None if the request failed.
    """
    url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
    try:
        response = client.get(url, timeout=30.0)
        if response.status_code != 200:
            logger.error(
                f"[PMC] Fetch XML failed for {pmcid} ({response.status_code})."
            )
            return None
        return response.text
    except httpx.HTTPError as error:
        logger.error(f"[PMC] XML request failed for {pmcid}: {error}.")
        return None


def extract_external_links(xml_content: str) -> list[str]:
    """Extract and deduplicate all HTTP and HTTPS URLs from XML tags and body text.

    Returns
    -------
        list[str]: Unique external links found in the publication.
    """
    if not xml_content or not isinstance(xml_content, str):
        return []
    soup = BeautifulSoup(xml_content, "xml")
    # Extract all HTTP and HTTPS URLs:
    # - \s  : stops at whitespace (spaces, tabs, newlines)
    # - < > : stops at XML/HTML tags and angle brackets
    # - ' " : stops at single and double quotes
    # - ( ) : stops at enclosing parentheses
    raw_urls = re.findall(
        r"https?://[^\s><'\"()]+", soup.get_text(), flags=re.IGNORECASE
    )
    # Clean and deduplicate URLs.
    cleaned_urls = set()
    cleaned_urls.update(url.rstrip(".,;()[]") for url in raw_urls if url)
    return list(cleaned_urls)


def parse_pmc_record(
    client: httpx.Client,
    raw_item: dict,
    logger: "loguru.Logger" = loguru.logger,
) -> PublicationMetadata | None:
    """Parse a raw Europe PMC record into a publication model.

    Returns
    -------
        PublicationMetadata | None: Validated paper record or None.
    """
    doi_val = raw_item.get("doi")
    title_val = raw_item.get("title")
    pmcid = str(raw_item.get("pmcid"))
    # Abort parsing if any mandatory bibliographic identifier is missing.
    if not doi_val or not title_val or not pmcid:
        return None
    # Clean and normalize the title, abstract, and DOI.
    clean_title = clean_markup_text(title_val)
    clean_abstract = clean_markup_text(raw_item.get("abstractText"))
    clean_doi = str(doi_val).strip().lower()
    # Parse author metadata.
    parsed_authors = parse_authors(
        raw_item.get("authorList", {}).get("author", []) or []
    )
    # Fetch and parse the full-text XML.
    xml_content = fetch_pmc_xml(client, pmcid, logger)
    # Filter out empty or null keywords from the raw list.
    raw_keywords = raw_item.get("keywordList", {}).get("keyword", [])
    clean_keywords = list(filter(None, raw_keywords))
    # Instantiate the Pydantic PublicationMetadata model.
    try:
        return PublicationMetadata(
            doi=clean_doi,
            publication_source_name=PublicationSourceName.EUROPE_PMC,
            publication_id_in_source=pmcid,
            url=f"https://doi.org/{clean_doi}",
            title=clean_title,
            authors=parsed_authors,
            year=str(raw_item.get("pubYear", "")),
            abstract=clean_abstract,
            journal=raw_item.get("journalInfo", {}).get("journal", {}).get("title"),
            keywords=clean_keywords,
            full_text_xml=xml_content,
            external_links=extract_external_links(xml_content),
        )
    except ValidationError as error:
        logger.error(f"[PMC] Schema error ({clean_doi}): {error}.")
        return None


def export_papers_to_parquet(
    dataframe: pd.DataFrame,
    new_papers: list[PublicationMetadata],
    filepath: Path,
    logger: "loguru.Logger" = loguru.logger,
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
    logger: "loguru.Logger" = loguru.logger,
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


def run_scraper_source(
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
    logger: "loguru.Logger" = loguru.logger,
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
    help="Scraper for MD papers via Europe PMC.",
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
def main(
    output_path: Path,
    nb_test: int | None,
    batch_size: int,
) -> None:
    """Scrape paper metadata from Europe PMC API."""
    level = "DEBUG" if nb_test else "INFO"
    logger = create_logger(logpath="logs/scrap_publications.log", level=level)
    logger.info("Starting Europe PMC publication scraping...")
    start_time = time.perf_counter()
    # Verify if parquet file exists and load existing DOIs.
    existing_df, existing_dois, existing_counts = (
        load_existing_publications_from_parquet(output_path)
    )
    logger.info(
        f"Loaded {len(existing_dois)} total existing papers from {output_path.name}."
    )
    # Create HTTPX client.
    load_dotenv()
    client = create_httpx_client()
    # Run scraper for Europe PMC.
    pending_batch = []
    total_scraped = 0
    existing_df, pending_batch, count_pmc = run_scraper_source(
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
        export_papers_to_parquet(existing_df, pending_batch, output_path, logger)
    elapsed = str(timedelta(seconds=time.perf_counter() - start_time)).split(".")[0]
    logger.info(f"Total new papers saved: {total_scraped} to: {output_path}.")
    logger.success(f"Successfully completed scraping in {elapsed}.")


if __name__ == "__main__":
    main()
