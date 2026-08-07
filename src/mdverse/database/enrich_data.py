"""Enrichment functions for publications, AI models, and datasets metadata."""

import re
from typing import Any

import defusedxml.ElementTree as ET  # noqa: N817
import httpx
import loguru
import numpy as np
import pandas as pd
from pydantic import ValidationError

from mdverse.models.ai_model import AiModelMetadata
from mdverse.models.dataset import DatasetMetadata
from mdverse.models.enums import DatasetSourceName, ExternalDatabaseName
from mdverse.scrapers.network import create_httpx_client

CROSSREF_API_URL = "https://api.crossref.org/works/"
ARXIV_API_URL = "https://export.arxiv.org/api/query"
HF_BASE_URL = "https://huggingface.co"
DOI_PATTERN = re.compile(r"^10\.\d{4,9}/[-._;()/:A-Za-z0-9]+$")

SIMULATION_CATEGORY_MAPPING = [
    ("total_number_of_atoms", "NATOMS"),
    ("simulation_timesteps_in_fs", "STIMESTEP"),
    ("simulation_times", "STIME"),
    ("simulation_temperatures_in_kelvin", "STEMP"),
]


def resolve_enum_attr(enum_cls: type, key: str, attr: str) -> str | None:
    """Resolve enum attribute.

    Returns
    -------
    str | None
        The attribute value if found, otherwise None.
    """
    # Check if key is provided.
    if not key:
        return None
    key_upper = str(key).upper()
    # Try to match member directly.
    if key_upper in enum_cls.__members__:
        return getattr(enum_cls[key_upper], attr)
    # Fallback to value mapping.
    if key in enum_cls._value2member_map_:
        return getattr(enum_cls(key), attr)
    return None


def extract_doi(link: str) -> str | None:
    """Extract DOI string.

    Returns
    -------
    str | None
        The extracted DOI if valid, otherwise None.
    """
    # Validate the link existence.
    if not link:
        return None
    clean = str(link).strip().split("doi.org/")[-1]
    # Exclude specific repository links.
    if "zenodo" in clean.lower() or "figshare" in clean.lower():
        return None
    # Validate against regex pattern.
    if DOI_PATTERN.match(clean):
        return clean
    return None


def fetch_from_crossref(
    doi: str, logger: "loguru.Logger" = loguru.logger
) -> dict[str, Any] | None:
    """Fetch Crossref data.

    Returns
    -------
    dict[str, Any] | None
        A dictionary containing publication metadata, or None if the lookup fails.
    """
    url = f"{CROSSREF_API_URL}{doi}"
    client = create_httpx_client()
    try:
        # Perform HTTP request to Crossref.
        response = client.get(url, timeout=5)
        response.raise_for_status()
        data = response.json().get("message", {})
        titles = data.get("title", [])
        title = titles[0] if titles else None
        created = (
            data.get("published-print")
            or data.get("published-online")
            or data.get("created")
        )
        year = None
        # Extract publication year safely.
        if created and "date-parts" in created and created["date-parts"]:
            year = str(created["date-parts"][0][0])
        containers = data.get("container-title", [])
        journal = containers[0] if containers else None
        subjects = data.get("subject", [])
        authors = []
        # Process each author record.
        for a in data.get("author", []):
            given = a.get("given", "").strip()
            family = a.get("family", "").strip()
            full_name = f"{given} {family}".strip() or family or given
            # Skip empty names.
            if not full_name:
                continue
            orcid = None
            if a.get("ORCID"):
                orcid = a.get("ORCID").split("/")[-1].strip()
            affiliations = a.get("affiliation", [])
            aff = None
            if affiliations and isinstance(affiliations[0], dict):
                aff = affiliations[0].get("name")
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
    except (httpx.HTTPError, ValueError, TypeError, AttributeError) as e:
        # Log parsing or network errors without crashing.
        logger.debug(f"Crossref lookup failed for DOI {doi}: {e}")
        return None


def fetch_from_arxiv(
    arxiv_id: str, logger: "loguru.Logger" = loguru.logger
) -> dict[str, Any] | None:
    """Fetch arXiv data.

    Returns
    -------
    dict[str, Any] | None
        A dictionary containing arXiv publication metadata, or None if the lookup fails.
    """
    try:
        # Request XML data from ArXiv.
        response = httpx.get(ARXIV_API_URL, params={"id_list": arxiv_id}, timeout=5)
        response.raise_for_status()
        root = ET.fromstring(response.text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        entry = root.find("atom:entry", ns)
        # Ensure entry was found.
        if entry is None:
            return None
        title_elem = entry.find("atom:title", ns)
        title = None
        # Clean title text.
        if title_elem is not None and title_elem.text:
            title = title_elem.text.strip().replace("\n", " ")
        if not title or title.startswith("Error"):
            return None
        published_elem = entry.find("atom:published", ns)
        year = None
        if published_elem is not None and published_elem.text:
            year = published_elem.text[:4]
        summary_elem = entry.find("atom:summary", ns)
        abstract = summary_elem.text.strip() if summary_elem is not None else None
        authors = []
        # Parse all author nodes.
        for a in entry.findall("atom:author", ns):
            name_elem = a.find("atom:name", ns)
            if name_elem is not None and name_elem.text:
                full_name = name_elem.text.strip()
                parts = full_name.split(" ")
                first_name = parts[0] if len(parts) > 1 else None
                last_name = " ".join(parts[1:]) if len(parts) > 1 else full_name
                authors.append(
                    {
                        "full_name": full_name,
                        "first_name": first_name,
                        "last_name": last_name,
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
    except (httpx.HTTPError, ET.ParseError, AttributeError, ValueError) as e:
        # Handle network or safe XML parsing issues.
        logger.debug(f"arXiv API lookup failed for ID {arxiv_id}: {e}")
        return None


def fetch_publication_metadata(
    doi: str, logger: "loguru.Logger" = loguru.logger
) -> dict[str, Any]:
    """Fetch metadata.

    Returns
    -------
    dict[str, Any]
        A dictionary containing the combined metadata. It will fallback to default.
    """
    # Sanitize incoming DOI string.
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
    # Check for ArXiv format.
    arxiv_match = re.search(r"10\.48550/arXiv\.(.+)$", clean_doi, re.IGNORECASE)
    if arxiv_match:
        arxiv_data = fetch_from_arxiv(arxiv_match.group(1), logger=logger)
        return {**default_meta, **(arxiv_data or {})}
    # Fallback to Crossref.
    data = fetch_from_crossref(clean_doi, logger=logger)
    if not data and "arXiv." in clean_doi:
        arxiv_id = clean_doi.split("arXiv.")[-1]
        data = fetch_from_arxiv(arxiv_id, logger=logger)
    return {**default_meta, **(data or {})}


def resolve_hf_model(
    client: httpx.Client, model_id: str, logger: "loguru.Logger"
) -> AiModelMetadata | None:
    """Fetch HF model.

    Returns
    -------
    AiModelMetadata | None
        A validated AiModelMetadata object, or None if the fetch fails.
    """
    url = f"{HF_BASE_URL}/api/models/{model_id}"
    try:
        # Execute the HTTP fetch.
        resp = client.get(url)
        if resp.status_code != 200:
            return None
        data = resp.json()
        card_data = data.get("cardData") or {}
        raw_tags = data.get("tags", [])
        keywords_list = (
            [str(tag) for tag in raw_tags] if isinstance(raw_tags, list) else []
        )
        license_val = card_data.get("license")
        # Extract license from tags if missing.
        if not license_val:
            for tag in keywords_list:
                if tag.startswith("license:"):
                    license_val = tag.replace("license:", "")
                    break
        safetensors_info = data.get("safetensors", {})
        param_count = (
            safetensors_info.get("total")
            if isinstance(safetensors_info, dict)
            else None
        )
        # Return validated Pydantic model.
        return AiModelMetadata(
            repository_name=DatasetSourceName.HUGGINGFACE,
            model_id_in_repository=model_id,
            model_url=f"https://huggingface.co/{model_id}",
            description=data.get("description"),
            license=license_val,
            tasks=data.get("pipeline_tag"),
            number_of_parameters=param_count,
            keywords=keywords_list,
            date_created=data.get("createdAt"),
            date_last_updated=data.get("lastModified"),
            doi=card_data.get("doi"),
        )
    except (httpx.HTTPError, ValueError, ValidationError, TypeError) as err:
        # Catch unexpected payload structures safely.
        logger.error(f"Failed resolving HF model {model_id}: {err}")
        return None


def resolve_hf_dataset(
    client: httpx.Client, dataset_id: str, logger: "loguru.Logger"
) -> DatasetMetadata | None:
    """Fetch HF dataset.

    Returns
    -------
    DatasetMetadata | None
        A validated DatasetMetadata object, or None if the fetch fails.
    """
    url = f"{HF_BASE_URL}/api/datasets/{dataset_id}"
    try:
        # Fetch dataset configuration.
        resp = client.get(url)
        if resp.status_code != 200:
            return None
        data = resp.json()
        card_data = data.get("cardData") or {}
        raw_tags = data.get("tags", [])
        keywords_list = [str(t) for t in raw_tags] if isinstance(raw_tags, list) else []
        license_val = card_data.get("license")
        # Retrieve license from dataset tags.
        if not license_val:
            for t in keywords_list:
                if t.startswith("license:"):
                    license_val = t.split("license:")[-1]
                    break
        doi_val = None
        # Locate DOI identifier.
        for t in keywords_list:
            if t.startswith("doi:"):
                doi_val = t.split("doi:")[-1]
                break
        created_at = data.get("createdAt")
        date_created = created_at[:10] if created_at else None
        last_modified = data.get("lastModified")
        date_last_updated = last_modified[:10] if last_modified else None
        siblings = data.get("siblings", [])
        file_number = len(siblings) if isinstance(siblings, list) else None
        # Return structured metadata.
        return DatasetMetadata(
            dataset_repository_name=DatasetSourceName.HUGGINGFACE,
            dataset_id_in_repository=dataset_id,
            dataset_url_in_repository=f"https://huggingface.co/datasets/{dataset_id}",
            title=data.get("id") or dataset_id,
            description=data.get("description"),
            doi=doi_val,
            date_created=date_created,
            date_last_updated=date_last_updated,
            number_of_files=file_number,
            license=license_val,
            download_number=data.get("downloads"),
            view_number=data.get("likes"),
            keywords=keywords_list,
        )
    except (httpx.HTTPError, ValueError, ValidationError, TypeError) as err:
        # Suppress fatal errors on missing endpoints.
        logger.error(f"Failed resolving HF dataset {dataset_id}: {err}")
        return None


def enrich_publication_models(
    client: httpx.Client,
    model_ids: list[Any],
    logger: "loguru.Logger" = loguru.logger,
) -> list[AiModelMetadata]:
    """Resolve AI models.

    Returns
    -------
    list[AiModelMetadata]
        A list of successfully resolved model metadata objects.
    """
    resolved_models = []
    # Loop over provided model identifiers.
    for m_id in model_ids:
        clean_id = None
        if isinstance(m_id, str):
            clean_id = m_id
        elif isinstance(m_id, dict):
            clean_id = m_id.get("model_id_in_repository") or m_id.get("id")
        elif hasattr(m_id, "model_id_in_repository"):
            clean_id = m_id.model_id_in_repository
        # Process the clean string if valid.
        if clean_id:
            meta = resolve_hf_model(client, clean_id, logger)
            if meta:
                resolved_models.append(meta)
    return resolved_models


def enrich_publications_and_authors(
    staged_publications_df: pd.DataFrame, logger: "loguru.Logger" = loguru.logger
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Enrich publications.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        A tuple containing two DataFrames for enriched publications and authors.
    """
    expected_cols = [
        "data_source_label",
        "id_in_data_source",
        "doi",
        "title",
        "year",
        "url",
        "abstract",
        "journal",
        "keywords",
    ]
    # Return empty frames immediately.
    if staged_publications_df.empty:
        return pd.DataFrame(columns=expected_cols), pd.DataFrame()
    enriched_records, authors = [], []
    # Iterate dynamically.
    for row in staged_publications_df.to_dict("records"):
        doi = row["doi"]
        meta = fetch_publication_metadata(doi, logger=logger) or {}
        authors.extend(meta.pop("authors", []) or [])
        enriched_records.append(
            {
                "data_source_label": row["data_source_label"],
                "id_in_data_source": row["id_in_data_source"],
                "doi": doi,
                "title": meta.get("title"),
                "year": meta.get("year"),
                "url": meta.get("url"),
                "abstract": meta.get("abstract"),
                "journal": meta.get("journal"),
                "keywords": meta.get("keywords", ""),
            }
        )
    # Cast elements back to pandas.
    enriched_df = pd.DataFrame(enriched_records, columns=expected_cols)
    authors_df = pd.DataFrame(authors) if authors else pd.DataFrame()
    return enriched_df, authors_df


def _extract_author_records(df: pd.DataFrame) -> pd.DataFrame:
    """Extract individual author records from the datasets DataFrame.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing individual author records with relevant metadata.
    """
    if "authors" not in df.columns or df["authors"].empty:
        return pd.DataFrame(
            columns=[
                "data_source_label",
                "id_in_data_source",
                "full_name",
                "orcid",
                "first_name",
                "last_name",
                "affiliation",
            ]
        )
    authors_df = (
        df[["data_source_label", "id_in_data_source", "authors"]]
        .dropna(subset=["authors"])
        .copy()
    )
    exploded = authors_df.explode("authors").dropna(subset=["authors"])

    if exploded.empty:
        return pd.DataFrame()
    authors_records = pd.json_normalize(exploded["authors"])
    authors_records["data_source_label"] = exploded["data_source_label"].to_numpy()
    authors_records["id_in_data_source"] = exploded["id_in_data_source"].to_numpy()
    target_columns = [
        "data_source_label",
        "id_in_data_source",
        "full_name",
        "orcid",
        "first_name",
        "last_name",
        "affiliation",
    ]
    for col in target_columns:
        if col not in authors_records.columns:
            authors_records[col] = None

    result = authors_records[target_columns].drop_duplicates()
    return result


def _extract_publication_records(df: pd.DataFrame) -> pd.DataFrame:
    """Extract valid DOIs.

    Returns
    -------
    pd.DataFrame
        A DataFrame mapping data sources to external publication DOIs.
    """
    records = []
    cols = ["id_in_data_source", "data_source_label", "external_links"]
    # Process only populated rows.
    for row in df[cols].dropna(subset=["external_links"]).to_dict("records"):
        links = row.get("external_links")
        if links is not None and hasattr(links, "__len__") and len(links) > 0:
            for link in links:
                doi = extract_doi(link)
                if doi:
                    records.append(
                        {
                            "data_source_label": str(row["data_source_label"]),
                            "id_in_data_source": str(row["id_in_data_source"]),
                            "doi": str(doi),
                        }
                    )
    return pd.DataFrame(
        records, columns=["data_source_label", "id_in_data_source", "doi"]
    ).drop_duplicates(subset=["doi"], keep="first")


def _extract_software_ffm_records(sim: dict, base: dict) -> list[dict]:
    """Extract software.

    Returns
    -------
    list[dict]
        A list of dictionaries representing software and forcefield annotations.
    """
    recs = []
    sw_items = sim.get("software")
    # Scan software properties.
    if sw_items is not None and hasattr(sw_items, "__len__") and len(sw_items) > 0:
        for item in sw_items:
            item_dict = item.model_dump() if hasattr(item, "model_dump") else item
            if isinstance(item_dict, dict):
                name = item_dict.get("name")
                if name:
                    recs.append(
                        {**base, "category_label": "SOFTNAME", "value": str(name)}
                    )
                version = item_dict.get("version")
                if version:
                    recs.append(
                        {**base, "category_label": "SOFTVERS", "value": str(version)}
                    )
    ffm_items = sim.get("forcefields_models")
    # Scan forcefields configurations.
    if ffm_items is not None and hasattr(ffm_items, "__len__") and len(ffm_items) > 0:
        for item in ffm_items:
            item_dict = item.model_dump() if hasattr(item, "model_dump") else item
            if isinstance(item_dict, dict):
                name = item_dict.get("name")
                if name:
                    recs.append({**base, "category_label": "FFM", "value": str(name)})
    return recs


def _extract_scalar_records(sim: dict, base: dict) -> list[dict]:
    """Extract scalars.

    Returns
    -------
    list[dict]
        A list of dictionaries representing numerical scalar annotations.
    """
    recs = []
    # Use mapping to pull properties safely.
    for key, cat in SIMULATION_CATEGORY_MAPPING:
        vals = sim.get(key)
        if vals is None:
            continue
        val_list = (
            vals
            if hasattr(vals, "__len__") and not isinstance(vals, (str, bytes))
            else [vals]
        )
        if hasattr(val_list, "__len__") and len(val_list) > 0:
            for val in val_list:
                if val is not None and str(val).strip():
                    recs.append({**base, "category_label": cat, "value": str(val)})
    return recs


def _extract_molecule_records(
    sim: dict, base: dict, ds: str, src_id: str
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Extract molecules.

    Returns
    -------
    tuple[list[dict], list[dict], list[dict], list[dict]]
        A tuple containing lists for: annotations, molecules, external database links,
        and database definitions.
    """
    ann_recs, mol_recs, ext_recs, ref_recs = [], [], [], []
    mols = sim.get("molecules")
    # Return immediately on invalid sets.
    if mols is None or not hasattr(mols, "__len__") or len(mols) == 0:
        return ann_recs, mol_recs, ext_recs, ref_recs
    for mol in mols:
        m = mol.model_dump() if hasattr(mol, "model_dump") else mol
        if not isinstance(m, dict):
            continue
        mname = m.get("name")
        if not mname:
            continue
        temp_id = f"{ds}_{src_id}_{mname}"
        ann_recs.append(
            {**base, "category_label": "MOL", "value": mname, "temp_mol_id": temp_id}
        )
        mol_recs.append(
            {
                "temp_mol_id": temp_id,
                "name": mname,
                "formula": m.get("formula"),
                "sequence": m.get("sequence") or "",
                "organism": m.get("organism"),
                "molecule_type_label": m.get("type"),
            }
        )
        ext_ids = m.get("external_identifiers")
        # Extract foreign database keys.
        if ext_ids is not None and hasattr(ext_ids, "__len__") and len(ext_ids) > 0:
            for ext in ext_ids:
                e = ext.model_dump() if hasattr(ext, "model_dump") else ext
                if isinstance(e, dict) and e.get("identifier"):
                    db_name = e.get("database_name")
                    db_lbl = (
                        db_name.value if hasattr(db_name, "value") else str(db_name)
                    )
                    ext_recs.append(
                        {
                            "temp_mol_id": temp_id,
                            "database_label": db_lbl,
                            "id_in_external_database": e.get("identifier"),
                            "url_in_external_database": e.get("url"),
                        }
                    )
                    ref_recs.append(
                        {
                            "database_label": db_lbl,
                            "url": resolve_enum_attr(
                                ExternalDatabaseName, db_lbl, "url"
                            )
                            or "",
                            "comment": resolve_enum_attr(
                                ExternalDatabaseName, db_lbl, "comment"
                            ),
                        }
                    )
    return ann_recs, mol_recs, ext_recs, ref_recs


def _extract_simulation_records(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Extract simulations.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
        A tuple of DataFrames for: annotations, molecules, external links, and
        database definitions.
    """
    ann_recs, mol_recs, ext_recs, ref_recs = [], [], [], []
    cols = ["id_in_data_source", "data_source_label", "simulation"]
    # Iterate efficiently through defined entries.
    for row in df[cols].dropna(subset=["simulation"]).to_dict("records"):
        sim = row["simulation"]
        sim = sim.model_dump() if hasattr(sim, "model_dump") else sim
        if not isinstance(sim, dict):
            continue
        ds = row["data_source_label"]
        src_id = row["id_in_data_source"]
        prov = (
            "Manually_annotated"
            if str(ds).lower() == "atlas"
            else "Provided_by_database"
        )
        base = {
            "id_in_data_source": src_id,
            "data_source_label": ds,
            "provenance_label": prov,
            "quality_score": 1.0,
        }
        # Aggregate multiple record formats.
        ann_recs.extend(_extract_software_ffm_records(sim, base))
        ann_recs.extend(_extract_scalar_records(sim, base))
        m_anns, m_details, m_exts, m_refs = _extract_molecule_records(
            sim, base, ds, src_id
        )
        ann_recs.extend(m_anns)
        mol_recs.extend(m_details)
        ext_recs.extend(m_exts)
        ref_recs.extend(m_refs)
    return (
        pd.DataFrame(
            ann_recs,
            columns=[
                "id_in_data_source",
                "data_source_label",
                "category_label",
                "value",
                "provenance_label",
                "quality_score",
                "temp_mol_id",
            ],
        ),
        pd.DataFrame(
            mol_recs,
            columns=[
                "temp_mol_id",
                "name",
                "formula",
                "sequence",
                "organism",
                "molecule_type_label",
            ],
        ),
        pd.DataFrame(
            ext_recs,
            columns=[
                "temp_mol_id",
                "database_label",
                "id_in_external_database",
                "url_in_external_database",
            ],
        ),
        pd.DataFrame(
            ref_recs, columns=["database_label", "url", "comment"]
        ).drop_duplicates(subset=["database_label"]),
    )


def _extract_pub_authors(row: dict) -> list[dict]:
    """Extract row authors.

    Returns
    -------
    list[dict]
        A list of author records.
    """
    author_recs = []
    doi = row.get("doi")
    ds_label = row.get("data_source_label")
    id_in_source = row.get("id_in_data_source")
    authors = row.get("authors")
    # Traverse author list strictly.
    if authors is not None and hasattr(authors, "__iter__"):
        for auth in authors:
            try:
                a = auth.model_dump() if hasattr(auth, "model_dump") else auth
                if isinstance(a, dict) and a.get("full_name"):
                    author_recs.append(
                        {
                            "data_source_label": ds_label,
                            "id_in_data_source": id_in_source,
                            "doi": doi,
                            "full_name": a.get("full_name"),
                            "orcid": a.get("orcid"),
                            "first_name": a.get("first_name"),
                            "last_name": a.get("last_name"),
                            "affiliation": a.get("affiliation"),
                        }
                    )
            except (ValueError, TypeError, AttributeError):
                continue
    return author_recs


def _extract_pub_models(
    row: dict, client: httpx.Client, logger: "loguru.Logger"
) -> tuple[list[dict], list[dict]]:
    """Extract row models.

    Returns
    -------
    tuple[list[dict], list[dict]]
        A tuple containing model records and publication-model link records.
    """
    model_recs = []
    link_recs = []
    doi = row.get("doi")
    ds_label = row.get("data_source_label")
    id_in_source = row.get("id_in_data_source")
    model_ids = (
        row.get("model_ids") or row.get("linked_models") or row.get("model_references")
    )
    # Cast cleanly into collections.
    if model_ids is not None:
        if isinstance(model_ids, np.ndarray):
            model_ids = model_ids.tolist()
        elif not isinstance(model_ids, (list, tuple)):
            model_ids = [model_ids]
    if not model_ids:
        return model_recs, link_recs
    try:
        resolved_models = enrich_publication_models(client, model_ids, logger=logger)
        # Process and link valid models.
        for m in resolved_models:
            m_dict = m.model_dump()
            kw_list = m_dict.get("keywords")
            kw_str = (
                " ;".join(kw_list) if isinstance(kw_list, list) else str(kw_list or "")
            )
            model_recs.append(
                {
                    "data_source_label": m_dict["repository_name"],
                    "id_in_data_source": m_dict["model_id_in_repository"],
                    "url_in_data_source": m_dict["model_url"],
                    "doi": m_dict.get("doi"),
                    "description": m_dict.get("description"),
                    "license": m_dict.get("license"),
                    "tasks": m_dict.get("tasks"),
                    "number_of_parameters": m_dict.get("number_of_parameters"),
                    "keywords": kw_str,
                    "date_created": m_dict.get("date_created"),
                    "date_last_updated": m_dict.get("date_last_updated"),
                    "date_last_fetched": m_dict.get("date_last_fetched"),
                }
            )
            link_recs.append(
                {
                    "pub_data_source_label": ds_label,
                    "pub_id_in_data_source": id_in_source,
                    "doi": doi,
                    "model_data_source_label": m_dict["repository_name"],
                    "model_id_in_data_source": m_dict["model_id_in_repository"],
                }
            )
    except (httpx.HTTPError, ValueError, ValidationError, TypeError) as err:
        logger.error(f"Failed extracting models for publication {doi}: {err}")
    return model_recs, link_recs


def _normalize_refs(refs: Any) -> list[Any]:
    """Normalize refs.

    Returns
    -------
    list[Any]
        A standardized list of references, or an empty list if None.
    """
    # Ensure iterable standard type.
    if refs is None:
        return []
    if isinstance(refs, np.ndarray):
        return refs.tolist()
    if not isinstance(refs, (list, tuple)):
        return [refs]
    return list(refs)


def _resolve_hf_dataset_record(
    client: httpx.Client, clean_ds_id: str, logger: "loguru.Logger"
) -> dict | None:
    """Resolve dataset.

    Returns
    -------
    dict | None
        The formatted dataset metadata dictionary, or None if the resolution fails.
    """
    # Retrieve base payload safely.
    ds_meta = resolve_hf_dataset(client, clean_ds_id, logger)
    if not ds_meta:
        return None
    md = ds_meta.model_dump()
    kw_list = md.get("keywords")
    kw_str = " ;".join(kw_list) if isinstance(kw_list, list) else str(kw_list or "")
    # Cast to final database mapping format.
    return {
        "data_source_label": md["dataset_repository_name"],
        "id_in_data_source": md["dataset_id_in_repository"],
        "url_in_data_source": md["dataset_url_in_repository"],
        "title": md.get("title"),
        "description": md.get("description"),
        "doi": md.get("doi"),
        "date_created": md.get("date_created"),
        "date_last_updated": md.get("date_last_updated"),
        "file_number": md.get("number_of_files"),
        "license": md.get("license"),
        "download_number": md.get("download_number"),
        "view_number": md.get("view_number"),
        "keywords": kw_str,
        "date_last_fetched": md.get("date_last_fetched"),
    }


def _extract_pub_datasets(
    row: dict, client: httpx.Client, logger: "loguru.Logger", seen_datasets: set
) -> tuple[list[dict], list[dict]]:
    """Extract row datasets.

    Returns
    -------
    tuple[list[dict], list[dict]]
        Publication-dataset link records and resolved dataset records.
    """
    pub_dataset_link_recs = []
    dataset_recs = []
    doi = row.get("doi")
    ds_label = row.get("data_source_label")
    id_in_source = row.get("id_in_data_source")
    dataset_refs = _normalize_refs(row.get("dataset_references"))
    # Process dynamically assigned collections.
    for ds_ref in dataset_refs:
        try:
            ds_dict = ds_ref.model_dump() if hasattr(ds_ref, "model_dump") else ds_ref
            if not isinstance(ds_dict, dict):
                continue
            raw_ds_id = ds_dict.get("dataset_id_in_repository", "")
            if not raw_ds_id:
                continue
            clean_ds_id = raw_ds_id.removeprefix("dataset:")
            repo_name = ds_dict.get("dataset_repository_name")
            # Build relational matrix structure.
            pub_dataset_link_recs.append(
                {
                    "pub_data_source_label": ds_label,
                    "pub_id_in_data_source": id_in_source,
                    "doi": doi,
                    "dataset_data_source_label": repo_name,
                    "dataset_id_in_data_source": clean_ds_id,
                }
            )
            ds_key = (str(repo_name).upper(), clean_ds_id)
            if ds_key in seen_datasets or ds_key[0] != "HUGGINGFACE":
                continue
            seen_datasets.add(ds_key)
            resolved_record = _resolve_hf_dataset_record(client, clean_ds_id, logger)
            if resolved_record:
                dataset_recs.append(resolved_record)
        except (
            httpx.HTTPError,
            ValueError,
            ValidationError,
            AttributeError,
            TypeError,
        ) as err:
            logger.error(
                f"Failed parsing dataset reference for publication {doi}: {err}"
            )
    return pub_dataset_link_recs, dataset_recs


def extract_publications_relational_data(
    df: pd.DataFrame, client: httpx.Client, logger: "loguru.Logger"
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Extract relations.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
        A tuple of DataFrames for:
        authors, AI models, links between publications and models, links between
        publications and datasets, and resolved dataset metadata.
    """
    author_recs = []
    model_recs = []
    link_recs = []
    pub_dataset_link_recs = []
    dataset_recs = []
    seen_datasets = set()
    # Batch processing initialization.
    for row in df.to_dict("records"):
        author_recs.extend(_extract_pub_authors(row))
        row_model_recs, row_link_recs = _extract_pub_models(row, client, logger)
        model_recs.extend(row_model_recs)
        link_recs.extend(row_link_recs)
        row_pub_ds_recs, row_ds_recs = _extract_pub_datasets(
            row, client, logger, seen_datasets
        )
        pub_dataset_link_recs.extend(row_pub_ds_recs)
        dataset_recs.extend(row_ds_recs)
    # Assemble structured containers.
    authors_df = pd.DataFrame(
        author_recs,
        columns=[
            "data_source_label",
            "id_in_data_source",
            "doi",
            "full_name",
            "orcid",
            "first_name",
            "last_name",
            "affiliation",
        ],
    )
    models_df = pd.DataFrame(
        model_recs,
        columns=[
            "data_source_label",
            "id_in_data_source",
            "url_in_data_source",
            "doi",
            "description",
            "license",
            "tasks",
            "number_of_parameters",
            "keywords",
            "date_created",
            "date_last_updated",
            "date_last_fetched",
        ],
    ).drop_duplicates(subset=["data_source_label", "id_in_data_source"])
    links_df = pd.DataFrame(
        link_recs,
        columns=[
            "pub_data_source_label",
            "pub_id_in_data_source",
            "doi",
            "model_data_source_label",
            "model_id_in_data_source",
        ],
    )
    pub_datasets_df = pd.DataFrame(
        pub_dataset_link_recs,
        columns=[
            "pub_data_source_label",
            "pub_id_in_data_source",
            "doi",
            "dataset_data_source_label",
            "dataset_id_in_data_source",
        ],
    ).drop_duplicates()
    resolved_datasets_df = pd.DataFrame(
        dataset_recs,
        columns=[
            "data_source_label",
            "id_in_data_source",
            "url_in_data_source",
            "title",
            "description",
            "doi",
            "date_created",
            "date_last_updated",
            "file_number",
            "license",
            "download_number",
            "view_number",
            "keywords",
            "date_last_fetched",
        ],
    ).drop_duplicates(subset=["data_source_label", "id_in_data_source"])
    return authors_df, models_df, links_df, pub_datasets_df, resolved_datasets_df
