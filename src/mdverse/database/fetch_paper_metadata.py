import re
import xml.etree.ElementTree as ET
from typing import Any

import httpx
import loguru

CROSSREF_API_URL = "https://api.crossref.org/works/"
ARXIV_API_URL = "https://export.arxiv.org/api/query"


def fetch_from_crossref(
    doi: str, email: str, logger: "loguru.Logger" = loguru.logger
) -> dict[str, Any] | None:
    """Fetch paper metadata from the Crossref REST API."""
    url = f"{CROSSREF_API_URL}{doi}"
    headers = {"User-Agent": f"MDverseDataCatalogue/1.0 (mailto:{email})"}
    try:
        response = httpx.get(url, headers=headers, timeout=5)
        response.raise_for_status()
        data = response.json().get("message", {})

        # Extract basic publication fields
        titles = data.get("title", [])
        title = titles[0] if titles else None

        created = (
            data.get("published-print")
            or data.get("published-online")
            or data.get("created")
        )
        year = (
            str(created["date-parts"][0][0])
            if created and "date-parts" in created and created["date-parts"]
            else None
        )

        containers = data.get("container-title", [])
        journal = containers[0] if containers else None
        subjects = data.get("subject", [])

        # Extract and format author records
        authors = []
        for a in data.get("author", []):
            given, family = a.get("given", "").strip(), a.get("family", "").strip()
            full_name = f"{given} {family}".strip() or family or given
            if not full_name:
                continue

            orcid = a.get("ORCID").split("/")[-1].strip() if a.get("ORCID") else None
            affiliations = a.get("affiliation", [])
            aff = (
                affiliations[0].get("name")
                if affiliations and isinstance(affiliations[0], dict)
                else None
            )

            authors.append(
                {
                    "full_name": full_name,
                    "first_name": given or None,
                    "last_name": family or None,
                    "orcid": orcid,
                    "affiliation": aff,
                }
            )

        return {
            "title": title,
            "year": year,
            "abstract": data.get("abstract"),
            "journal": journal,
            "keywords": " ;".join(subjects) if subjects else None,
            "authors": authors,
        }
    except Exception as e:
        logger.debug(f"Crossref lookup failed for DOI {doi}: {e}")
        return None


def fetch_from_arxiv(
    arxiv_id: str, logger: "loguru.Logger" = loguru.logger
) -> dict[str, Any] | None:
    """Fetch paper metadata from the arXiv XML API."""
    try:
        response = httpx.get(ARXIV_API_URL, params={"id_list": arxiv_id}, timeout=5)
        response.raise_for_status()

        root = ET.fromstring(response.text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        entry = root.find("atom:entry", ns)
        if entry is None:
            return None

        title_elem = entry.find("atom:title", ns)
        title = (
            title_elem.text.strip().replace("\n", " ")
            if title_elem is not None
            else None
        )
        if not title or title.startswith("Error"):
            return None

        published_elem = entry.find("atom:published", ns)
        year = (
            published_elem.text[:4]
            if published_elem is not None and published_elem.text
            else None
        )

        summary_elem = entry.find("atom:summary", ns)
        abstract = summary_elem.text.strip() if summary_elem is not None else None

        authors = []
        for a in entry.findall("atom:author", ns):
            name_elem = a.find("atom:name", ns)
            if name_elem is not None and name_elem.text:
                full_name = name_elem.text.strip()
                parts = full_name.split(" ")
                authors.append(
                    {
                        "full_name": full_name,
                        "first_name": parts[0] if len(parts) > 1 else None,
                        "last_name": (
                            " ".join(parts[1:]) if len(parts) > 1 else full_name
                        ),
                        "orcid": None,
                        "affiliation": None,
                    }
                )

        categories = [
            c.attrib["term"]
            for c in entry.findall("atom:category", ns)
            if "term" in c.attrib
        ]

        return {
            "title": title,
            "year": year,
            "abstract": abstract,
            "journal": f"arXiv:{arxiv_id}",
            "keywords": " ;".join(categories) if categories else None,
            "authors": authors,
        }
    except Exception as e:
        logger.debug(f"arXiv API lookup failed for ID {arxiv_id}: {e}")
        return None


def fetch_paper_metadata(
    doi: str, email: str, logger: "loguru.Logger" = loguru.logger
) -> dict[str, Any]:
    """Fetch metadata for a given DOI by routing to arXiv or Crossref APIs."""
    clean_doi = (
        str(doi).strip().split("doi.org/")[-1]
        if "doi.org/" in str(doi)
        else str(doi).strip()
    )
    default_meta = {
        "doi": clean_doi,
        "title": None,
        "year": None,
        "url": f"https://doi.org/{clean_doi}",
        "abstract": None,
        "journal": None,
        "keywords": None,
        "authors": [],
    }

    # Route 1: Directly query arXiv API if DOI matches arXiv pattern (10.48550/arXiv...)
    if arxiv_match := re.search(r"10\.48550/arXiv\.(.+)$", clean_doi, re.IGNORECASE):
        arxiv_data = fetch_from_arxiv(arxiv_match.group(1), logger=logger)
        return {**default_meta, **(arxiv_data or {})}
    # Route 2: Query Crossref API with arXiv fallback if Crossref fails/returns 404
    data = fetch_from_crossref(clean_doi, email, logger=logger)
    if not data and "arXiv." in clean_doi:
        arxiv_id = clean_doi.split("arXiv.")[-1]
        data = fetch_from_arxiv(arxiv_id, logger=logger)

    return {**default_meta, **(data or {})}
