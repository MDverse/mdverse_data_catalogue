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
import loguru
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
from mdverse.scrapers.enrich_papers import enrich_paper_record
from mdverse.scrapers.network import (
    create_httpx_client,
    is_connection_to_server_working,
)

# Endpoints.
EUROPE_PMC_PING_URL = (
    "https://www.ebi.ac.uk/europepmc/webservices/rest/search?"
    "query=test&format=json&pageSize=1"
)
HF_PING_URL = "https://huggingface.co/api/papers/search?q=test&limit=1"
EUROPE_PMC_SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
EUROPE_PMC_FULLTEXT_URL = (
    "https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
)
HF_BASE_URL = "https://huggingface.co"

# Queries & Keywords.
EUROPE_PMC_SEARCH_QUERY = (
    "("
    'TITLE:("molecular dynamics" AND (simulation* OR trajector* OR dataset*))'
    ") "
    "AND OPEN_ACCESS:y "
    "AND HAS_FT:y "
    "NOT TITLE:review* "
    'NOT PUB_TYPE:"Review" '
    'NOT PUB_TYPE:"review-article" '
    'NOT PUB_TYPE:"Systematic Review"'
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
HF_PAPERS_SEARCH_QUERY = "molecular dynamics"


def load_existing_publications_from_parquet(
    parquet_path: Path,
) -> tuple[pd.DataFrame, set[str], dict[str, int]]:
    """Load existing Parquet DataFrame, set of DOIs, and counts per source.

    Returns
    -------
        tuple[pd.DataFrame, set[str], dict[str, int]]:
            DataFrame, existing DOIs, and counts per publication source.
    """
    if parquet_path.exists():
        dataframe = pd.read_parquet(parquet_path)
        existing_dois = set(dataframe["doi"].dropna().str.lower())

        counts = {}
        if "publication_source_name" in dataframe.columns:
            source_counts = dataframe["publication_source_name"].value_counts()
            for source_enum in PublicationSourceName:
                counts[source_enum.value] = int(source_counts.get(source_enum.value, 0))
        return dataframe, existing_dois, counts
    return pd.DataFrame(), set(), {source.value: 0 for source in PublicationSourceName}


def safe_get(
    client: httpx.Client,
    url: str,
    params: dict | None = None,
    max_retries: int = 3,
) -> httpx.Response:
    """Execute GET request with exponential backoff on HTTP 429 status code.

    Returns
    -------
        httpx.Response: The HTTP response object.
    """
    for attempt in range(max_retries):
        response = client.get(url, params=params)
        if response.status_code == 429:
            # Respect Retry-After header if provided, otherwise exponential backoff
            retry_after = response.headers.get("Retry-After")
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
    """Extract section header title and its Markdown depth level.

    Returns
    -------
        tuple[str | None, int]: Lowercase header title and depth level (1-6).
    """
    stripped = line.strip()
    # Inline ATX headers (e.g. text.### Title)
    if "#" in stripped and not stripped.startswith("#"):
        hash_index = stripped.find("#")
        potential_header = stripped[hash_index:]
        if re.match(r"^#{1,6}\s+", potential_header):
            stripped = potential_header
    # Standard ATX headers (# Title, ## 4. Methods)
    if re.match(r"^#{1,6}\s+", stripped):
        header_text = stripped.lstrip("#").strip().lower()
        level = len(stripped) - len(stripped.lstrip("#"))
        return header_text, level
    # Bold header style (e.g. **Methods** or **4 Methods**)
    if re.match(r"^\*\*\d*(\.\d+)*\s*.*?\*\*", stripped):
        header_text = stripped.strip("*").strip().lower()
        return header_text, 2
    # Setext headers (Title followed by --- or === on next line)
    if next_line:
        next_stripped = next_line.strip()
        if next_stripped and set(next_stripped) == {"="}:
            return stripped.lower(), 1
        if next_stripped and set(next_stripped) == {"-"}:
            return stripped.lower(), 2
    return None, 0


def _extract_methods_fallback(lines: list[str]) -> str | None:
    """Fallback extractor using regex when structured headers are missing.

    Returns
    -------
        str | None: Extracted Methods content or None.
    """
    methods_lines = []
    is_in_section = False
    for line in lines:
        stripped = line.strip()
        lower_line = stripped.lower()
        # Check if line looks like a un-markdowned header for methods
        is_methods_start = (
            any(keyword in lower_line for keyword in METHODS_KEYWORDS)
            and len(stripped) < 60
        )
        if is_methods_start and not is_in_section:
            is_in_section = True
            methods_lines.append(line)
            continue
        if is_in_section:
            # Stop if encountering obvious next major sections
            if (
                any(
                    lower_line.startswith(stop_kw)
                    for stop_kw in ["results", "discussion", "references", "ackno"]
                )
                and len(stripped) < 40
            ):
                break
            methods_lines.append(line)
    text = "\n".join(methods_lines).strip()
    return text or None


def _extract_methods_structured(lines: list[str]) -> str | None:
    """Extract methods section based on structured Markdown headers.

    Returns
    -------
        str | None: Extracted Methods text or None if not found.
    """
    methods_lines = []
    is_in_section, skip_next_line = False, False
    section_level = 0
    for index, line in enumerate(lines):
        if skip_next_line:
            skip_next_line = False
            continue
        next_line = lines[index + 1] if index + 1 < len(lines) else None
        header, level = _extract_header_info(line, next_line)
        if header:
            clean_header = re.sub(r"^\d+(\.\d+)*\.?\s*", "", header).strip()
            if any(keyword in clean_header for keyword in METHODS_KEYWORDS):
                is_in_section = True
                section_level = level
                methods_lines.append(line)
                if next_line and (
                    set(next_line.strip()) == {"-"} or set(next_line.strip()) == {"="}
                ):
                    methods_lines.append(next_line)
                    skip_next_line = True
                continue
            if is_in_section and level <= section_level:
                break
        if is_in_section:
            methods_lines.append(line)

    text = "\n".join(methods_lines).strip()
    return text or None


def extract_materials_and_methods_hf(
    client: httpx.Client, paper_id: str, logger: "loguru.Logger"
) -> str | None:
    """Fetch full-text Markdown for Hugging Face paper and extract Methods.

    Returns
    -------
        str | None: Extracted Methods section text or None.
    """
    url = f"https://huggingface.co/papers/{paper_id}.md"
    try:
        response = safe_get(client, url)
        if response.status_code != 200:
            if response.status_code == 404:
                logger.warning(
                    f"[HF] No Markdown full-text found for paper {paper_id} (HTTP 404)."
                )
                return None
            logger.error(
                f"[HF] Could not fetch Markdown full-text for paper "
                f"{paper_id} (HTTP {response.status_code})."
            )
            return None
    except httpx.HTTPError as err:
        logger.error(f"[HF] Methods extraction failed for {paper_id}: {err}.")
        return None
    else:
        lines = response.text.splitlines()
        extracted_text = _extract_methods_structured(lines)
        if not extracted_text:
            extracted_text = _extract_methods_fallback(lines)

        if not extracted_text:
            logger.warning(
                f"[HF] No 'Materials and Methods' section found in "
                f"Markdown for paper {paper_id}."
            )
            return None
        return extracted_text


def search_hf_papers(
    client: httpx.Client, limit: int | None, logger: "loguru.Logger"
) -> list[dict]:
    """Search Hugging Face papers via search API.

    Returns
    -------
        list[dict]: List of paper records.
    """
    url = f"{HF_BASE_URL}/api/papers/search"
    params = {"q": HF_PAPERS_SEARCH_QUERY, "limit": limit or 120}
    response = safe_get(client, url, params=params)
    if response.status_code != 200:
        logger.error(f"[HF] Search failed with HTTP {response.status_code}.")
        return []
    return response.json()


def fetch_hf_linked_datasets(
    client: httpx.Client, paper_id: str
) -> list[DatasetCoreMetadata]:
    """Fetch datasets linked to an arXiv ID on Hugging Face.

    Returns
    -------
        list[DatasetCoreMetadata]: List of dataset references.
    """
    response = client.get(
        f"{HF_BASE_URL}/api/datasets", params={"filter": f"arxiv:{paper_id}"}
    )
    if response.status_code != 200:
        return []

    dataset_refs = []
    for item in response.json():
        dataset_id = item.get("id") or item.get("_id")
        if dataset_id:
            with contextlib.suppress(ValidationError):
                dataset_refs.append(
                    DatasetCoreMetadata(
                        dataset_repository_name=DatasetSourceName.HUGGINGFACE,
                        dataset_id_in_repository=str(dataset_id),
                        dataset_url_in_repository=(
                            f"https://huggingface.co/datasets/{dataset_id}"
                        ),
                    )
                )
    return dataset_refs


def fetch_hf_linked_models(
    client: httpx.Client, paper_id: str
) -> list[AiModelCoreMetadata]:
    """Fetch AI models linked to an arXiv ID on Hugging Face.

    Returns
    -------
        list[AiModelCoreMetadata]: List of model references.
    """
    response = client.get(
        f"{HF_BASE_URL}/api/models", params={"filter": f"arxiv:{paper_id}"}
    )
    if response.status_code != 200:
        return []

    model_refs = []
    for item in response.json():
        model_id = item.get("id") or item.get("_id")
        if model_id:
            with contextlib.suppress(ValidationError):
                model_refs.append(
                    AiModelCoreMetadata(
                        repository_name=DatasetSourceName.HUGGINGFACE,
                        model_id_in_repository=str(model_id),
                        model_url=f"https://huggingface.co/{model_id}",
                    )
                )
    return model_refs


def parse_authors(authors_data: list[dict]) -> list[Person]:
    """Extract author names.

    Returns
    -------
        list[Person]: List of validated Person models.
    """
    persons = []
    for author in authors_data:
        full_name = author.get("name")
        if not full_name:
            continue
        name_parts = full_name.strip().split()
        first_name = name_parts[0] if name_parts else None
        last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else None
        persons.append(
            Person(
                first_name=first_name,
                last_name=last_name,
                full_name=full_name,
            )
        )
    return persons


def parse_hf_datasets(linked_datasets: list[dict]) -> list[DatasetCoreMetadata]:
    """Build DatasetCoreMetadata instances from Hugging Face linked datasets.

    Returns
    -------
        list[DatasetCoreMetadata]: List of validated dataset metadata.
    """
    datasets = []
    for item in linked_datasets:
        dataset_id = item.get("id")
        if not dataset_id:
            continue
        datasets.append(
            DatasetCoreMetadata(
                dataset_repository_name=DatasetSourceName.HUGGINGFACE,
                dataset_id_in_repository=str(dataset_id),
                dataset_url_in_repository=f"https://huggingface.co/datasets/{dataset_id}",
            )
        )
    return datasets


def parse_hf_models(linked_models: list[dict]) -> list[AiModelCoreMetadata]:
    """Build AiModelCoreMetadata instances from Hugging Face linked models.

    Returns
    -------
        list[AiModelCoreMetadata]: List of validated AI model metadata.
    """
    models = []
    for item in linked_models:
        model_id = item.get("id")
        if not model_id:
            continue
        models.append(
            AiModelCoreMetadata(
                repository_name=DatasetSourceName.HUGGINGFACE,
                model_id_in_repository=str(model_id),
                model_url=f"https://huggingface.co/{model_id}",
            )
        )
    return models


def parse_hf_record(
    client: httpx.Client, raw_item: dict, logger: "loguru.Logger"
) -> PublicationMetadata | None:
    """Parse raw Hugging Face paper JSON into PublicationMetadata.

    Returns
    -------
        PublicationMetadata | None: Validated paper model or None.
    """
    paper_id = raw_item.get("id") or raw_item.get("paper", {}).get("id")
    title = raw_item.get("title") or raw_item.get("paper", {}).get("title")
    if not paper_id or not title:
        return None
    time.sleep(0.5)
    resp = safe_get(client, f"{HF_BASE_URL}/api/papers/{paper_id}")
    paper_details = resp.json() if resp.status_code == 200 else raw_item
    arxiv_doi = f"10.48550/arxiv.{paper_id}".lower()
    pub_date = paper_details.get("publishedAt") or ""
    publication_year = str(pub_date)[:4]
    linked_datasets = parse_hf_datasets(paper_details.get("linkedDatasets", []))
    linked_models = parse_hf_models(paper_details.get("linkedModels", []))
    methods_text = extract_materials_and_methods_hf(client, str(paper_id), logger)
    try:
        return PublicationMetadata(
            doi=arxiv_doi,
            publication_source_name=PublicationSourceName.HUGGINGFACE,
            publication_id_in_source=str(paper_id),
            url=f"https://huggingface.co/papers/{paper_id}",
            title=title,
            year=publication_year,
            keywords=paper_details.get("ai_keywords", []),
            authors=parse_authors(paper_details.get("authors", [])),
            abstract=paper_details.get("summary"),
            journal="Hugging Face Daily Papers",
            materials_and_methods=methods_text,
            dataset_references=linked_datasets,
            model_references=linked_models,
        )
    except ValidationError as err:
        logger.error(f"[HF] Schema error ({paper_id}): {err}.")
        return None


def search_europe_pmc_stream(
    client: httpx.Client,
    max_results: int | None,
) -> Generator[list[dict]]:
    """Yield pages of open-access MD papers from Europe PMC.

    Yields
    ------
        list[dict]: Page chunk of raw paper metadata records.
    """
    cursor_mark = "*"
    page_size = 50
    total_yielded = 0
    while True:
        params = {
            "query": EUROPE_PMC_SEARCH_QUERY,
            "format": "json",
            "pageSize": page_size,
            "resultType": "core",
            "cursorMark": cursor_mark,
        }
        # Make the GET request to Europe PMC API
        response = client.get(
            EUROPE_PMC_SEARCH_URL,
            params=params,
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()
        results = data.get("resultList", {}).get("result", [])
        if not results:
            break
        # Yield the current page of results
        yield results
        # Update the total number of results yielded
        total_yielded += len(results)
        # Check if we have reached the maximum number of results
        # or if there are no more pages
        next_cursor = data.get("nextCursorMark")
        if (max_results and total_yielded >= max_results) or (
            next_cursor == cursor_mark
        ):
            break
        cursor_mark = next_cursor


def _extract_authors_pmc(raw_item: dict) -> list[Person]:
    """Extract author metadata from Europe PMC raw item.

    Returns
    -------
        list[Person]: List of validated Person models.
    """
    authors_data = raw_item.get("authorList", {}).get("author", []) or []
    return [
        Person(
            first_name=auth.get("firstName"),
            last_name=auth.get("lastName"),
            full_name=auth.get("fullName"),
            # Use next() to extract the first element
            # without raising StopIteration if not found.
            orcid=next(
                (
                    aid.get("value")
                    for aid in [auth.get("authorId") or {}]
                    if aid.get("type") == "ORCID"
                ),
                None,
            ),
            affiliation=next(
                (
                    aff.get("affiliation")
                    for aff in auth.get("authorAffiliationDetailsList", {}).get(
                        "authorAffiliation", []
                    )
                    or []
                ),
                None,
            ),
        )
        for auth in authors_data
    ]


def _extract_repository_links(*texts: str | None) -> list[str]:
    """Extract repository links from given texts using regex pattern.

    Returns
    -------
        list[str]: List of unique repository URLs found in the texts.
    """
    found_links = set()
    for text in texts:
        if not text:
            continue
        matches = REPOSITORY_URL_PATTERN.findall(text)
        for match in matches:
            clean_url = match.rstrip(".,;()[]")
            found_links.add(clean_url)

    return list(found_links)


def _extract_methods_and_links_pmc(
    client: httpx.Client, pmcid: str, logger: "loguru.Logger"
) -> tuple[str | None, list[str]]:
    """Fetch XML full-text, extract Methods text and repository links.

    Returns
    -------
        tuple[str | None, list[str]]:
            - Extracted Materials and Methods section text (or None).
            - List of unique repository URLs found in the entire XML text.
    """
    try:
        # Fetch the full-text XML for the given PMCID
        url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
        res = client.get(url, timeout=30.0)
        if res.status_code != 200:
            logger.error(
                f"[PMC] Could not fetch XML full-text for {pmcid} "
                f"(HTTP {res.status_code})."
            )
            return None, []
    except httpx.HTTPError as err:
        logger.error(f"[PMC] Failed fetching XML full-text for {pmcid}: {err}.")
        return None, []
    else:
        # Parse XML raw bytes using lxml XML parser
        soup = BeautifulSoup(res.content, "xml")
        # 1. Extract repository URLs (GitHub, Zenodo, Figshare, etc.)
        full_text = soup.get_text()
        extracted_links = _extract_repository_links(full_text)
        # 2. Extract Methods sections matching defined keywords
        kw_pattern = re.compile("|".join(METHODS_KEYWORDS), re.IGNORECASE)
        methods = [
            "\n".join(p.get_text(strip=True) for p in sec.find_all("p"))
            for sec in soup.find_all("sec")
            if kw_pattern.search(f"{sec.get('sec-type', '')} {sec.find('title')}")
        ]
        # Join the extracted methods sections into a single string
        methods_text = "\n\n".join(filter(None, methods)).strip() or None
        if not methods_text:
            logger.warning(
                f"[PMC] No 'Materials and Methods' section found in XML "
                f"for paper {pmcid}."
            )

        return methods_text, extracted_links


def parse_pmc_record(
    client: httpx.Client, raw_item: dict, logger: "loguru.Logger"
) -> PublicationMetadata | None:
    """Parse raw Europe PMC JSON into PublicationMetadata.

    Returns
    -------
        PublicationMetadata | None: Validated paper record or None.
    """
    doi_val = raw_item.get("doi")
    title_val = raw_item.get("title")
    pub_year = str(raw_item.get("pubYear", ""))
    pmcid = raw_item.get("pmcid")
    if not doi_val or not title_val or not pub_year or not pmcid:
        return None
    clean_doi = str(doi_val).strip().lower()
    journal_info = raw_item.get("journalInfo", {}).get("journal", {})
    keywords = raw_item.get("keywordList", {}).get("keyword", [])
    abstract_text = raw_item.get("abstractText")
    methods_text, external_links = _extract_methods_and_links_pmc(
        client, str(pmcid), logger
    )
    try:
        return PublicationMetadata(
            doi=clean_doi,
            publication_source_name=PublicationSourceName.EUROPE_PMC,
            publication_id_in_source=str(pmcid),
            url=f"https://doi.org/{clean_doi}",
            title=title_val,
            authors=_extract_authors_pmc(raw_item),
            year=pub_year,
            abstract=abstract_text,
            journal=journal_info.get("title"),
            keywords=keywords,
            materials_and_methods=methods_text,
            external_links=external_links,
        )
    except ValidationError as err:
        logger.error(f"[PMC] Schema error ({clean_doi}): {err}.")
        return None


def scrape_europe_pmc_source(
    client: httpx.Client,
    existing_df: pd.DataFrame,
    existing_dois: set[str],
    output_path: Path,
    nb_test: int | None,
    batch_size: int,
    pending_batch: list[PublicationMetadata],
    logger: "loguru.Logger",
) -> tuple[pd.DataFrame, list[PublicationMetadata], int]:
    """Scrape papers from Europe PMC.

    Returns
    -------
    tuple[pd.DataFrame, list[PublicationMetadata], int]:
        Updated DataFrame, pending batch, and count of scraped papers.
    """
    logger.info("=== Scraping Europe PMC ===")
    pmc_count = 0
    for page_results in search_europe_pmc_stream(client, max_results=None):
        for raw_item in page_results:
            if nb_test and pmc_count >= nb_test:
                break

            doi_val = raw_item.get("doi")
            if not doi_val or str(doi_val).strip().lower() in existing_dois:
                continue

            paper = parse_pmc_record(client, raw_item, logger)
            if paper:
                pmc_count += 1
                existing_dois.add(paper.doi)
                pending_batch.append(paper)
                logger.info(f"[PMC] Scraped #{pmc_count} ({paper.doi})")
                if len(pending_batch) >= batch_size:
                    existing_df = export_papers_to_parquet(
                        existing_df, pending_batch, output_path, client, logger
                    )
                    pending_batch = []

        if nb_test and pmc_count >= nb_test:
            break

    return existing_df, pending_batch, pmc_count


def scrape_huggingface_source(
    client: httpx.Client,
    existing_df: pd.DataFrame,
    existing_dois: set[str],
    output_path: Path,
    nb_test: int | None,
    batch_size: int,
    pending_batch: list[PublicationMetadata],
    logger: "loguru.Logger",
) -> tuple[pd.DataFrame, list[PublicationMetadata], int]:
    """Scrape papers from Hugging Face Papers.

    Returns
    -------
    tuple[pd.DataFrame, list[PublicationMetadata], int]:
        Updated DataFrame, pending batch, and count of scraped papers.
    """
    logger.info("=== Scraping Hugging Face Papers ===")
    hf_count = 0
    hf_results = search_hf_papers(client, limit=nb_test, logger=logger)
    for raw_item in hf_results:
        if nb_test and hf_count >= nb_test:
            break

        paper_id = raw_item.get("id") or raw_item.get("paper", {}).get("id")
        if not paper_id:
            continue

        arxiv_doi = f"10.48550/arxiv.{paper_id}".lower()
        if arxiv_doi in existing_dois:
            continue

        paper = parse_hf_record(client, raw_item, logger)
        if paper:
            hf_count += 1
            existing_dois.add(paper.doi)
            pending_batch.append(paper)
            logger.info(
                f"[HF] Scraped #{hf_count} ({paper.doi}) | "
                f"Datasets: {len(paper.dataset_references)} | "
                f"Models: {len(paper.model_references)}"
            )
            if len(pending_batch) >= batch_size:
                existing_df = export_papers_to_parquet(
                    existing_df, pending_batch, output_path, client, logger
                )
                pending_batch = []
    return existing_df, pending_batch, hf_count


def export_papers_to_parquet(
    existing_df: pd.DataFrame,
    new_papers: list[PublicationMetadata],
    output_path: Path,
    client: httpx.Client,
    logger: "loguru.Logger",
) -> pd.DataFrame:
    """Enrich and export combined paper records to Parquet file.

    Returns
    -------
        pd.DataFrame: Updated combined DataFrame.
    """
    enriched_papers = [
        enrich_paper_record(client, paper, logger) for paper in new_papers
    ]
    new_data = [paper.model_dump() for paper in enriched_papers]
    new_df = pd.DataFrame(new_data)

    if not existing_df.empty:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=["doi"], keep="first")
    else:
        combined_df = new_df

    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_parquet(output_path, index=False)
    logger.success(f"Batch saved to {output_path.name}.")
    return combined_df


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
    hf_scraped_count = 0
    pmc_scraped_count = 0
    if use_hf:
        # Check connection to source servers before proceeding.
        if is_connection_to_server_working(client, HF_PING_URL, logger=logger):
            logger.success("Connection to Hugging Face API successful!")
            existing_df, pending_batch, hf_scraped_count = scrape_huggingface_source(
                client=client,
                existing_df=existing_df,
                existing_dois=existing_dois,
                output_path=output_path,
                nb_test=nb_test,
                batch_size=batch_size,
                pending_batch=pending_batch,
                logger=logger,
            )
        else:
            logger.warning(
                "Connection to Hugging Face API failed. Skipping Hugging Face source."
            )
        if hf_scraped_count == 0:
            logger.warning("[HF] No new papers scraped.")
            logger.warning(
                f"All fetched Hugging Face papers "
                f"are already present in {output_path.name} "
                f"({existing_counts.get(hf_source_key, 0)} existing)."
            )
    # Execute Source: Europe PMC.
    if use_pmc:
        if is_connection_to_server_working(client, EUROPE_PMC_PING_URL, logger=logger):
            logger.success("Connection to Europe PMC API successful!")
            existing_df, pending_batch, pmc_scraped_count = scrape_europe_pmc_source(
                client=client,
                existing_df=existing_df,
                existing_dois=existing_dois,
                output_path=output_path,
                nb_test=nb_test,
                batch_size=batch_size,
                pending_batch=pending_batch,
                logger=logger,
            )
        else:
            logger.warning(
                "Connection to Europe PMC API failed. Skipping Europe PMC source."
            )
        if pmc_scraped_count == 0:
            logger.warning("[PMC] No new papers scraped.")
            logger.warning(
                f"All fetched Europe PMC papers "
                f"are already present in {output_path.name} "
                f"({existing_counts.get(pmc_source_key, 0)} existing)."
            )
    # Final batch save if any pending papers remain.
    if pending_batch:
        existing_df = export_papers_to_parquet(
            existing_df, pending_batch, output_path, client, logger
        )
    total_scraped = pmc_scraped_count + hf_scraped_count
    elapsed = str(timedelta(seconds=time.perf_counter() - start_time)).split(".")[0]
    logger.info(f"Total new papers saved: {total_scraped} to: {output_path}.")
    logger.success(f"Successfully completed scraping in {elapsed}.")


if __name__ == "__main__":
    main()
