"""Scraper for molecular dynamics papers via Europe PMC and Hugging Face."""

import contextlib
import re
import time
from collections.abc import Generator
from datetime import timedelta
from pathlib import Path

import click
import defusedxml.ElementTree as element_tree  # noqa: N813
import httpx
import loguru
import pandas as pd
from pydantic import ValidationError

from mdverse.core.logger import create_logger
from mdverse.models.ai_model import AiModelCoreMetadata
from mdverse.models.dataset import DatasetCoreMetadata
from mdverse.models.enums import DatasetSourceName, PublicationSourceName
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
    '("molecular dynamics simulation" OR '
    '("molecular dynamics" AND ("simulated" OR "trajectories" OR "production"))'
    ") AND OPEN_ACCESS:y AND HAS_FT:y "
    'NOT TITLE:"review" NOT PUB_TYPE:"Review" '
    "sort_cited:y"
)
HF_PAPERS_SEARCH_QUERY = "molecular dynamics simulation"
METHODS_KEYWORDS = [
    "method",
    "materials and methods",
    "experimental procedures",
    "simulation details",
    "computational methods",
    "methodology",
]


def search_europe_pmc_stream(
    client: httpx.Client,
    max_results: int | None,
    logger: "loguru.Logger",
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
        response = client.get(EUROPE_PMC_SEARCH_URL, params=params)
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


def extract_materials_and_methods_pmc(
    client: httpx.Client, pmcid: str, logger: "loguru.Logger"
) -> str | None:
    """Fetch XML full-text for Europe PMC article and extract Methods text.

    Returns
    -------
        str | None: Extracted Methods section text or None if missing.
    """
    # Fetch XML full-text for the given PMCID
    url = EUROPE_PMC_FULLTEXT_URL.format(pmcid=pmcid)
    try:
        response = client.get(url)
        if response.status_code != 200:
            logger.error(
                f"[PMC] Could not fetch XML full-text for {pmcid} "
                f"(HTTP {response.status_code})."
            )
            return None
        # Parse XML and extract Methods sections
        root = element_tree.fromstring(response.content)
        methods_texts = []
        # Iterate through all <sec> elements to find Methods sections
        for section in root.findall(".//sec"):
            section_type = section.get("sec-type", "").lower()
            title_elem = section.find("title")
            title_text = (
                title_elem.text.lower()
                if title_elem is not None and title_elem.text
                else ""
            )
            # Check if the section is a Methods section based on keywords
            is_methods = any(
                keyword in section_type or keyword in title_text
                for keyword in ["method", "material", "simulation"]
            )
            if is_methods:
                paragraphs = [
                    paragraph.text.strip()
                    for paragraph in section.findall(".//p")
                    if paragraph.text and paragraph.text.strip()
                ]
                if paragraphs:
                    methods_texts.append("\n".join(paragraphs))
        # Combine all extracted Methods texts into a single string
        text = "\n\n".join(methods_texts).strip()
        if not text:
            logger.warning(
                f"[PMC] No 'Materials and Methods' section found in XML "
                f"for paper {pmcid}."
            )
            return None
        return text
    except (httpx.HTTPError, element_tree.ParseError) as err:
        logger.error(f"[PMC] Failed parsing XML full-text for {pmcid}: {err}.")
        return None


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
    methods_text = extract_materials_and_methods_pmc(client, str(pmcid), logger)

    try:
        return PublicationMetadata(
            doi=clean_doi,
            publication_source_name=PublicationSourceName.EUROPE_PMC,
            publication_id_in_source=str(pmcid),
            url=f"https://doi.org/{clean_doi}",
            title=title_val,
            authors=[],
            year=pub_year,
            abstract=raw_item.get("abstractText"),
            journal=journal_info.get("title"),
            keywords=[],
            simulation=None,
            dataset_references=[],
            model_references=[],
            materials_and_methods=methods_text,
        )
    except ValidationError as err:
        logger.error(f"[PMC] Schema error ({clean_doi}): {err}.")
        return None


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


def extract_materials_and_methods_arxiv(
    client: httpx.Client, paper_id: str, logger: "loguru.Logger"
) -> str | None:
    """Fetch full-text Markdown for arXiv paper and extract Methods.

    Returns
    -------
        str | None: Extracted Methods section text or None.
    """
    url = f"https://huggingface.co/papers/{paper_id}.md"
    try:
        response = client.get(url)
        if response.status_code != 200:
            logger.error(
                f"[HF] Could not fetch Markdown full-text for paper "
                f"{paper_id} (HTTP {response.status_code})."
            )
            return None

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
    except httpx.HTTPError as err:
        logger.error(f"[HF] Methods extraction failed for {paper_id}: {err}.")
        return None


def search_hf_papers(
    client: httpx.Client, limit: int, logger: "loguru.Logger"
) -> list[dict]:
    """Search Hugging Face papers via search API.

    Returns
    -------
        list[dict]: List of paper records.
    """
    url = f"{HF_BASE_URL}/api/papers/search"
    params = {"q": HF_PAPERS_SEARCH_QUERY, "limit": min(limit, 120)}
    response = client.get(url, params=params)
    response.raise_for_status()
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


def parse_hf_record(
    client: httpx.Client, raw_item: dict, logger: "loguru.Logger"
) -> PublicationMetadata | None:
    """Parse raw Hugging Face paper JSON into PublicationMetadata.

    Returns
    -------
        PublicationMetadata | None: Validated paper model or None.
    """
    paper_id = raw_item.get("id") or raw_item.get("paper", {}).get("id")
    title_val = raw_item.get("title") or raw_item.get("paper", {}).get("title")

    if not paper_id or not title_val:
        return None

    resp = client.get(f"{HF_BASE_URL}/api/papers/{paper_id}")
    details = resp.json() if resp.status_code == 200 else raw_item

    arxiv_doi = f"10.48550/arxiv.{paper_id}".lower()
    pub_date = details.get("publishedAt") or ""
    pub_year = str(pub_date)[:4] if pub_date else "2024"

    linked_datasets = fetch_hf_linked_datasets(client, str(paper_id))
    linked_models = fetch_hf_linked_models(client, str(paper_id))
    methods_text = extract_materials_and_methods_arxiv(client, str(paper_id), logger)

    try:
        return PublicationMetadata(
            doi=arxiv_doi,
            publication_source_name=PublicationSourceName.HUGGINGFACE,
            publication_id_in_source=str(paper_id),
            url=f"https://huggingface.co/papers/{paper_id}",
            title=title_val,
            authors=[],
            year=pub_year,
            abstract=details.get("summary"),
            journal="arXiv / Hugging Face Papers",
            keywords=[],
            simulation=None,
            dataset_references=linked_datasets,
            model_references=linked_models,
            materials_and_methods=methods_text,
        )
    except ValidationError as err:
        logger.error(f"[HF] Schema error ({paper_id}): {err}.")
        return None


def load_existing_papers(
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
    for page_results in search_europe_pmc_stream(
        client, max_results=None, logger=logger
    ):
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
    hf_limit = 120 if nb_test is None else max(nb_test * 3, 120)
    hf_results = search_hf_papers(client, limit=hf_limit, logger=logger)

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
    log_file = output_path.parent / "logs" / "scrape_papers.log"
    level = "DEBUG" if nb_test else "INFO"
    logger = create_logger(logpath=log_file, level=level)
    logger.info("Starting MDverse Paper Scraper.")
    start_time = time.perf_counter()
    # Verify if parquet file exists and load existing DOIs.
    existing_df, existing_dois, existing_counts = load_existing_papers(output_path)
    pmc_source_key = PublicationSourceName.EUROPE_PMC.value
    hf_source_key = PublicationSourceName.HUGGINGFACE.value
    logger.info(
        f"Loaded {len(existing_dois)} total existing papers from {output_path.name}."
    )
    # Create HTTPX client.
    client = create_httpx_client()
    # Execute Source: Hugging Face Papers.
    # Check connection to source servers before proceeding.
    pending_batch = []
    hf_scraped_count = 0
    pmc_scraped_count = 0
    if use_hf:
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
        if hf_scraped_count == 0:
            logger.warning("[HF] No new papers scraped.")
            logger.warning(
                f"All fetched Hugging Face papers "
                f"are already present in {output_path.name} "
                f"({existing_counts.get(hf_source_key, 0)} existing)."
            )
        else:
            logger.warning(
                "Connection to Hugging Face API failed. Skipping Hugging Face source."
            )
    # Execute Source: Europe PMC.
    # Check connection to source servers before proceeding.
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
        if pmc_scraped_count == 0:
            logger.warning("[PMC] No new papers scraped.")
            logger.warning(
                f"All fetched Europe PMC papers "
                f"are already present in {output_path.name} "
                f"({existing_counts.get(pmc_source_key, 0)} existing)."
            )
        else:
            logger.warning(
                "Connection to Europe PMC API failed. Skipping Europe PMC source."
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
