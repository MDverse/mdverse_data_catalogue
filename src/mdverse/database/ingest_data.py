"""Ingest scraped metadata into the MDverse DuckDB database."""

import re
import time
from datetime import timedelta
from functools import partial
from pathlib import Path

import click
import duckdb
import loguru
import numpy as np
import pandas as pd

from mdverse.core.logger import create_logger
from mdverse.database.fetch_paper_metadata import fetch_paper_metadata
from mdverse.models.enums import DatasetSourceName, ExternalDatabaseName

DATASETS_COLUMN_MAPPING = {
    "dataset_repository_name": "data_source_label",
    "dataset_id_in_repository": "id_in_data_source",
    "dataset_url_in_repository": "url_in_data_source",
    "dataset_project_name": "project_label",
    "dataset_id_in_project": "id_in_project",
    "dataset_url_in_project": "url_in_project",
    "number_of_files": "file_number",
}

SIMULATION_CATEGORY_MAPPING = [
    ("total_number_of_atoms", "NATOMS"),
    ("simulation_timesteps_in_fs", "STIMESTEP"),
    ("simulation_times", "STIME"),
    ("simulation_temperatures_in_kelvin", "STEMP"),
]

FILES_COLUMN_MAPPING = {
    "dataset_repository_name": "data_source_label",
    "dataset_id_in_repository": "dataset_id",
    "file_name": "name",
    "file_url_in_repository": "url",
    "file_size_in_bytes": "size_in_bytes",
    "file_md5": "md5",
    "containing_archive_file_name": "parent_zip_file_name",
    "file_type": "file_type_label",
    "file_size_with_human_readable_unit": "size_human_readable",
}

DOI_PATTERN = re.compile(r"^10\.\d{4,9}/[-._;()/:A-Za-z0-9]+$")


def init_db_connection(
    db_path: Path, *, read_only: bool = False, logger: "loguru.Logger" = loguru.logger
) -> duckdb.DuckDBPyConnection:
    """Initialize a connection to the DuckDB database.

    Returns
    -------
        duckdb.DuckDBPyConnection: Database connection object.
    """
    try:
        db = duckdb.connect(str(db_path), read_only=read_only)
        logger.success(f"Successfully connected to DuckDB database at {db_path}.")
        return db
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB database at {db_path}: {e}")
        raise


def resolve_enum_attr(enum_cls: type, key: str, attr: str) -> str | None:
    """Resolve an attribute from an enum by member name or value.

    Returns
    -------
        str | None: Resolved attribute value or None.
    """
    if not key:
        return None
    key_upper = str(key).upper()
    if key_upper in enum_cls.__members__:
        return getattr(enum_cls[key_upper], attr)
    if key in enum_cls._value2member_map_:
        return getattr(enum_cls(key), attr)
    return None


def load_datasets_df(parquet_path: str) -> pd.DataFrame:
    """Load and process datasets Parquet metadata into a DataFrame.

    Returns
    -------
        pd.DataFrame: Formatted datasets DataFrame.
    """
    df = pd.read_parquet(parquet_path).rename(columns=DATASETS_COLUMN_MAPPING)
    # Convert list-like keywords to a semicolon-separated string
    df["keywords"] = df["keywords"].apply(
        lambda r: " ;".join(r) if isinstance(r, (list, tuple, np.ndarray)) else ""
    )
    # Convert numeric columns to nullable integer type
    for col in ("file_number", "download_number", "view_number"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    # Resolve URLs, citations, and comments for data sources and projects
    for prefix, source_col in (
        ("data_source", "data_source_label"),
        ("project", "project_label"),
    ):
        for attr in ("url", "citation", "comment"):
            # Resolve enum attributes and create new columns
            df[f"{prefix}_{attr}"] = df[source_col].apply(
                partial(resolve_enum_attr, DatasetSourceName, attr=attr)
            )
    return df


def load_files_df(parquet_path: str) -> pd.DataFrame:
    """Load and process files Parquet metadata into a DataFrame.

    Returns
    -------
        pd.DataFrame: Formatted files DataFrame.
    """
    df = pd.read_parquet(
        parquet_path, columns=list(FILES_COLUMN_MAPPING.keys())
    ).rename(columns=FILES_COLUMN_MAPPING)
    # Add a binary column indicating if the file is from a zip archive
    df["is_from_zip_file"] = df["parent_zip_file_name"].notna().astype(int)
    if "size_in_bytes" in df.columns:
        df["size_in_bytes"] = pd.to_numeric(
            df["size_in_bytes"], errors="coerce"
        ).astype("Int64")
    return df


def extract_doi(link: str) -> str | None:
    """Extract and validate a DOI string.

    Returns
    -------
        str | None: Validated DOI or None.
    """
    if not link:
        return None
    clean = str(link).strip().split("doi.org/")[-1]
    # Reject DOIs from dataset repositories like Zenodo or Figshare
    if "zenodo" in clean.lower() or "figshare" in clean.lower():
        return None
    return clean if DOI_PATTERN.match(clean) else None


def _extract_author_records(df: pd.DataFrame) -> pd.DataFrame:
    """Extract unique author records linked to datasets.

    Returns
    -------
        pd.DataFrame: DataFrame containing author records.
    """
    records, seen_names, seen_orcids = [], set(), set()
    cols = ["id_in_data_source", "data_source_label", "authors"]
    for row in df[cols].dropna(subset=["authors"]).to_dict("records"):
        authors = row.get("authors")
        if authors is None or (hasattr(authors, "__len__") and len(authors) == 0):
            continue
        # Iterate through authors and extract unique records
        for auth in authors:
            a = auth.model_dump() if hasattr(auth, "model_dump") else auth
            if not isinstance(a, dict):
                continue
            name, orcid = a.get("full_name"), a.get("orcid")
            orcid = str(orcid).strip() if orcid else None
            # Skip if name is missing, already seen, or ORCID is already seen
            if not name or name in seen_names or (orcid and orcid in seen_orcids):
                continue
            seen_names.add(name)
            if orcid:
                seen_orcids.add(orcid)
            records.append(
                {
                    "data_source_label": row["data_source_label"],
                    "id_in_data_source": row["id_in_data_source"],
                    "full_name": name,
                    "orcid": orcid,
                    "first_name": a.get("first_name"),
                    "last_name": a.get("last_name"),
                    "affiliation": a.get("affiliation"),
                }
            )
    return pd.DataFrame(
        records,
        columns=[
            "data_source_label",
            "id_in_data_source",
            "full_name",
            "orcid",
            "first_name",
            "last_name",
            "affiliation",
        ],
    )


def _extract_paper_records(df: pd.DataFrame) -> pd.DataFrame:
    """Extract valid publication DOIs linked to datasets.

    Returns
    -------
        pd.DataFrame: DataFrame containing paper records.
    """
    records = []
    cols = ["id_in_data_source", "data_source_label", "external_links"]
    for row in df[cols].dropna(subset=["external_links"]).to_dict("records"):
        links = row.get("external_links")
        if links is not None and hasattr(links, "__len__") and len(links) > 0:
            for link in links:
                doi = extract_doi(link)
                if doi:
                    records.append(
                        {
                            "data_source_label": row["data_source_label"],
                            "id_in_data_source": row["id_in_data_source"],
                            "doi": doi,
                        }
                    )
    return pd.DataFrame(
        records, columns=["data_source_label", "id_in_data_source", "doi"]
    )


def _extract_software_ffm_records(sim: dict, base: dict) -> list[dict]:
    """Extract software (name/version) and forcefield records from simulation.

    Returns
    -------
        list[dict]: Extracted software and forcefield annotation records.
    """
    recs = []
    # Process software items: separate name (SOFTNAME) and version (SOFTVERS)
    sw_items = sim.get("software")
    if sw_items is not None and hasattr(sw_items, "__len__") and len(sw_items) > 0:
        for item in sw_items:
            # Handle Pydantic models or dicts safely
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
                        {
                            **base,
                            "category_label": "SOFTVERS",
                            "value": str(version),
                        }
                    )
    # Process forcefields/models: keep only the name (FFM)
    ffm_items = sim.get("forcefields_models")
    if ffm_items is not None and hasattr(ffm_items, "__len__") and len(ffm_items) > 0:
        for item in ffm_items:
            item_dict = item.model_dump() if hasattr(item, "model_dump") else item
            if isinstance(item_dict, dict):
                name = item_dict.get("name")
                if name:
                    recs.append({**base, "category_label": "FFM", "value": str(name)})

    return recs


def _extract_scalar_records(sim: dict, base: dict) -> list[dict]:
    """Extract scalar parameter annotations from simulation dictionary.

    Returns
    -------
        list[dict]: Extracted scalar parameter records.
    """
    recs = []
    for key, cat in SIMULATION_CATEGORY_MAPPING:
        vals = sim.get(key)
        if vals is None:
            continue
        # Convert scalar values (int, float) to list, or keep iterable
        if hasattr(vals, "__len__") and not isinstance(vals, (str, bytes)):
            val_list = vals
        else:
            val_list = [vals]
        # Safely iterate through elements
        if hasattr(val_list, "__len__") and len(val_list) > 0:
            for val in val_list:
                if val is not None and str(val).strip():
                    recs.append({**base, "category_label": cat, "value": str(val)})

    return recs


def _extract_molecule_records(
    sim: dict, base: dict, ds: str, src_id: str
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Extract molecule entities and external database reference records.

    Returns
    -------
        tuple[list[dict], list[dict], list[dict], list[dict]]:
            Annotation records, molecule details, external DB links, and DB refs.
    """
    ann_recs, mol_recs, ext_recs, ref_recs = [], [], [], []
    mols = sim.get("molecules")
    # Return empty lists if no molecules are present.
    if mols is None or not hasattr(mols, "__len__") or len(mols) == 0:
        return ann_recs, mol_recs, ext_recs, ref_recs
    # Iterate through molecules and extract relevant details.
    for mol in mols:
        m = mol.model_dump() if hasattr(mol, "model_dump") else mol
        if not isinstance(m, dict) or not (mname := m.get("name")):
            continue
        # Create a temporary molID for linking annotations and external references.
        temp_id = f"{ds}_{src_id}_{mname}"
        # Append molecule annotation record with category "MOL" and temporary ID.
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
        # Extract external database identifiers and create corresponding records.
        ext_ids = m.get("external_identifiers")
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
    """Extract simulation annotations, molecules, and DB references.

    Returns
    -------
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
            DataFrames for annotations, molecules, external DB links, and DB refs.
    """
    ann_recs, mol_recs, ext_recs, ref_recs = [], [], [], []
    cols = ["id_in_data_source", "data_source_label", "simulation"]
    # Iterate through rows with non-null simulation data and extract records.
    for row in df[cols].dropna(subset=["simulation"]).to_dict("records"):
        sim = row["simulation"]
        sim = sim.model_dump() if hasattr(sim, "model_dump") else sim
        if not isinstance(sim, dict):
            continue

        ds, src_id = row["data_source_label"], row["id_in_data_source"]
        # Determine provenance label based on data source.
        # (e.g., "Manually_annotated" for Atlas, otherwise "Provided_by_database").
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
        # Extract software, forcefield, and scalar parameter records.
        ann_recs.extend(_extract_software_ffm_records(sim, base))
        ann_recs.extend(_extract_scalar_records(sim, base))
        # Extract molecule records and related external database references.
        m_anns, m_details, m_exts, m_refs = _extract_molecule_records(
            sim, base, ds, src_id
        )
        # Append extracted molecule-related records to the main lists.
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


def enrich_papers_and_authors(
    staged_papers_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Enrich paper metadata via API and extract author records.

    Returns
    -------
        tuple[pd.DataFrame, pd.DataFrame]: Enriched paper and author DataFrames.
    """
    cols = ["doi", "title", "year", "url", "abstract", "journal", "keywords"]
    if staged_papers_df.empty:
        return pd.DataFrame(columns=cols), pd.DataFrame()
    enriched, authors = [], []
    # Iterate through unique DOIs and fetch metadata, extracting authors.
    for doi in staged_papers_df["doi"].dropna().unique():
        meta = fetch_paper_metadata(doi, email="")
        authors.extend(meta.pop("authors", []) or [])
        enriched.append(meta)
    return pd.DataFrame(enriched, columns=cols), pd.DataFrame(authors)


def ingest_datasets(
    df: pd.DataFrame,
    db_conn: duckdb.DuckDBPyConnection,
    sql_path: Path = Path("params/queries/ingest_datasets.sql"),
    logger: "loguru.Logger" = loguru.logger,
) -> list[int]:
    """Ingest datasets metadata and related entities into DuckDB.

    Returns
    -------
        list[int]: List of ingested dataset IDs.
    """
    # Register temporary views
    # for datasets, authors, papers, annotations, molecules, and external DB references.
    stage_authors = _extract_author_records(df)
    paper_records = _extract_paper_records(df)
    ann_df, mol_df, ext_db_df, ref_db_df = _extract_simulation_records(df)
    # Enrich paper metadata and extract author records via API.
    enriched_papers, crossref_authors = enrich_papers_and_authors(paper_records)
    if not crossref_authors.empty:
        stage_authors = pd.concat([stage_authors, crossref_authors], ignore_index=True)
    if not stage_authors.empty:
        stage_authors = stage_authors.drop_duplicates(
            subset=["full_name"], keep="first"
        )
    # Register temporary views for ingestion.
    views = {
        "_stage_datasets": df.drop(
            columns=["authors", "external_links"], errors="ignore"
        ),
        "_stage_authors": stage_authors,
        "_stage_papers": enriched_papers,
        "_stage_paper_links": paper_records,
        "_stage_annotations": ann_df,
        "_stage_molecules": mol_df,
        "_stage_molecules_ext_db": ext_db_df,
        "_stage_ref_databases": ref_db_df,
    }
    for name, view_df in views.items():
        db_conn.register(name, view_df)
    # Execute the ingestion SQL script and commit the transaction.
    try:
        db_conn.execute(sql_path.read_text(encoding="utf-8"))
        db_conn.commit()
    finally:
        # Unregister temporary views to clean up the database connection.
        for name in views:
            db_conn.unregister(name)
    # Retrieve the list of ingested dataset IDs based on the data source labels.
    sources = df["data_source_label"].unique().tolist()
    all_ids = [
        r[0]
        for r in db_conn.execute(
            """
            SELECT dataset_id FROM datasets
            WHERE data_source_label IN (SELECT unnest($1::VARCHAR[]))
            """,
            [sources],
        ).fetchall()
    ]
    logger.success(f"Ingested {len(all_ids):,} datasets successfully.")
    return all_ids


def ingest_files(
    df: pd.DataFrame,
    db_conn: duckdb.DuckDBPyConnection,
    sql_path: Path = Path("params/queries/ingest_files.sql"),
    logger: "loguru.Logger" = loguru.logger,
) -> None:
    """Ingest files metadata into DuckDB."""
    if df.empty:
        logger.warning("No files to ingest, skipping.")
        return
    # Register the files DataFrame as a temporary view for ingestion.
    db_conn.register("_stage_files", df)
    try:
        db_conn.execute(sql_path.read_text(encoding="utf-8"))
        db_conn.commit()
    finally:
        db_conn.unregister("_stage_files")
    logger.success(f"Ingested {len(df):,} files successfully.")


def ingest(
    db_path: Path,
    data_type: str,
    parquet_path: Path,
    logger: "loguru.Logger" = loguru.logger,
) -> None:
    """Route and execute data ingestion based on data type."""
    logger.info(f"Starting {data_type} ingestion into {db_path} from {parquet_path}.")
    db = init_db_connection(db_path, read_only=False, logger=logger)

    if data_type == "datasets":
        ingest_datasets(load_datasets_df(str(parquet_path)), db, logger=logger)
    elif data_type == "files":
        ingest_files(load_files_df(str(parquet_path)), db, logger=logger)

    db.close()


@click.command(help="Ingest data into the MDverse database.")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True, file_okay=True, path_type=Path),
    required=True,
    help="Path to DuckDB database file.",
)
@click.option(
    "--parquet",
    "parquet_path",
    type=click.Path(exists=True, file_okay=True, path_type=Path),
    required=True,
    help="Path to input Parquet file.",
)
@click.option(
    "--type",
    "data_type",
    type=click.Choice(["datasets", "files", "papers"], case_sensitive=False),
    required=True,
    help="Define data type to ingest.",
)
def main(db_path: Path, parquet_path: Path, data_type: str) -> None:
    """CLI entry point for data ingestion."""
    logger = create_logger(
        logpath=Path("logs/ingest_data_into_database.log"), level="INFO"
    )
    start_time = time.perf_counter()
    ingest(db_path, data_type, parquet_path, logger=logger)
    elapsed = str(timedelta(seconds=time.perf_counter() - start_time)).split(".")[0]
    logger.info(f"Total time: {elapsed}")
    logger.info("Done.")


if __name__ == "__main__":
    main()
