"""Ingest scraped metadata into the MDverse DuckDB database."""

import time
from datetime import timedelta
from functools import partial
from pathlib import Path

import click
import duckdb
import httpx
import loguru
import numpy as np
import pandas as pd

from mdverse.core.logger import create_logger
from mdverse.database.enrich_data import (
    _extract_author_records,
    _extract_publication_records,
    _extract_simulation_records,
    enrich_publications_and_authors,
    extract_doi,
    extract_publications_relational_data,
    resolve_enum_attr,
)
from mdverse.models.enums import DatasetSourceName, PublicationSourceName

DATASETS_COLUMN_MAPPING = {
    "dataset_repository_name": "data_source_label",
    "dataset_id_in_repository": "id_in_data_source",
    "dataset_url_in_repository": "url_in_data_source",
    "dataset_project_name": "project_label",
    "dataset_id_in_project": "id_in_project",
    "dataset_url_in_project": "url_in_project",
    "number_of_files": "file_number",
}

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

PUBLICATIONS_COLUMN_MAPPING = {
    "publication_source_name": "data_source_label",
    "publication_id_in_source": "id_in_data_source",
}


def init_db_connection(
    db_path: Path, *, read_only: bool = False, logger: "loguru.Logger" = loguru.logger
) -> duckdb.DuckDBPyConnection:
    """Initialize a connection to the DuckDB database.

    Returns
    -------
    duckdb.DuckDBPyConnection
        A connection object to the DuckDB database.
    """
    try:
        db = duckdb.connect(str(db_path), read_only=read_only)
        logger.success(f"Successfully connected to DuckDB database at {db_path}.")
        return db
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB database at {db_path}: {e}")
        raise


def format_keywords(keyword_list: list[str] | tuple[str] | np.ndarray) -> str:
    """Format a list of keywords into a semicolon-separated string.

    Returns
    -------
    str
        A semicolon-separated string of keywords, or an empty string
        if the input is not a list, tuple, or numpy array.
    """
    if isinstance(keyword_list, (list, tuple, np.ndarray)):
        return " ;".join(keyword_list)
    return ""


def _process_external_links_and_datasets(row: pd.Series) -> pd.Series:
    """Separate Zenodo/Figshare links from external_links into dataset_references.

    Returns
    -------
    pd.Series
        A Series containing the updated external_links and dataset_references.
    """
    links = row.get("external_links")
    ds_refs = row.get("dataset_references")

    ext_links = []
    new_ds_refs = (
        list(ds_refs) if isinstance(ds_refs, (list, tuple, np.ndarray)) else []
    )

    if isinstance(links, (list, tuple, np.ndarray)):
        for link in links:
            if not link:
                continue
            l_str = str(link).strip()
            l_lower = l_str.lower()
            if "zenodo" in l_lower or "figshare" in l_lower:
                ds_label = (
                    DatasetSourceName.ZENODO
                    if "zenodo" in l_lower
                    else DatasetSourceName.FIGSHARE
                )
                doi = extract_doi(l_str)
                ds_id = doi or l_str.split("/")[-1]
                new_ds_refs.append(
                    {
                        "dataset_repository_name": ds_label,
                        "dataset_id_in_repository": ds_id,
                        "dataset_url_in_repository": l_str,
                    }
                )
            else:
                ext_links.append(l_str)
    elif isinstance(links, str) and links.strip():
        ext_links.append(links.strip())

    return pd.Series([ext_links, new_ds_refs])


def load_datasets_df(parquet_path: str) -> pd.DataFrame:
    """Load and process datasets Parquet metadata into a DataFrame.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the processed datasets metadata.
    """
    df = pd.read_parquet(parquet_path).rename(columns=DATASETS_COLUMN_MAPPING)
    df["keywords"] = df["keywords"].apply(format_keywords)
    for col in ("file_number", "download_number", "view_number"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for prefix, source_col in (
        ("data_source", "data_source_label"),
        ("project", "project_label"),
    ):
        for attr in ("url", "citation", "comment"):
            df[f"{prefix}_{attr}"] = df[source_col].apply(
                partial(resolve_enum_attr, DatasetSourceName, attr=attr)
            )
    return df


def load_files_df(parquet_path: str) -> pd.DataFrame:
    """Load and process files Parquet metadata into a DataFrame.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the processed files metadata.
    """
    df = pd.read_parquet(parquet_path).rename(columns=FILES_COLUMN_MAPPING)
    df["is_from_zip_file"] = df["parent_zip_file_name"].notna().astype(int)
    if "size_in_bytes" in df.columns:
        df["size_in_bytes"] = pd.to_numeric(
            df["size_in_bytes"], errors="coerce"
        ).astype("Int64")
    return df


def load_publications_df(parquet_path: str) -> pd.DataFrame:
    """Load and process publications Parquet metadata into a DataFrame.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the processed publications metadata.
    """
    df = pd.read_parquet(parquet_path).rename(columns=PUBLICATIONS_COLUMN_MAPPING)
    df["keywords"] = df["keywords"].apply(format_keywords)

    # Separation of external_links and dataset_references.
    if "external_links" in df.columns:
        if "dataset_references" not in df.columns:
            df["dataset_references"] = np.empty((len(df), 0)).tolist()
        df[["external_links", "dataset_references"]] = df.apply(
            _process_external_links_and_datasets, axis=1
        )
    else:
        df["external_links"] = [[] for _ in range(len(df))]

    for attr in ("url", "citation", "comment"):
        df[f"data_source_{attr}"] = df["data_source_label"].apply(
            partial(resolve_enum_attr, PublicationSourceName, attr=attr)
        )
    return df


def ingest_datasets(
    df: pd.DataFrame,
    db_conn: duckdb.DuckDBPyConnection,
    sql_path: Path = Path("params/queries/ingest_datasets.sql"),
    logger: "loguru.Logger" = loguru.logger,
) -> list[int]:
    """Ingest datasets metadata and related entities into DuckDB.

    Returns
    -------
    list[int]
        A list of dataset IDs that were successfully ingested.
    """
    stage_authors = _extract_author_records(df)
    publication_records = _extract_publication_records(df)
    ann_df, mol_df, ext_db_df, ref_db_df = _extract_simulation_records(df)
    enriched_publications, crossref_authors = enrich_publications_and_authors(
        publication_records
    )
    # Merge crossref authors with stage authors, ensuring no duplicates.
    if not crossref_authors.empty:
        stage_authors = pd.concat([stage_authors, crossref_authors], ignore_index=True)
    if not stage_authors.empty:
        stage_authors["full_name"] = stage_authors["full_name"].astype(str).str.strip()
    views = {
        "_stage_datasets": df.drop(
            columns=["authors", "external_links"], errors="ignore"
        ),
        "_stage_authors": stage_authors,
        "_stage_publications": enriched_publications,
        "_stage_publication_links": publication_records,
        "_stage_annotations": ann_df,
        "_stage_molecules": mol_df,
        "_stage_molecules_ext_db": ext_db_df,
        "_stage_ref_databases": ref_db_df,
    }
    for name, view_df in views.items():
        db_conn.register(name, view_df)
    try:
        db_conn.execute(sql_path.read_text(encoding="utf-8"))
        db_conn.commit()
    finally:
        for name in views:
            db_conn.unregister(name)

    sources = df["data_source_label"].unique().tolist()
    all_ids = [
        r[0]
        for r in db_conn.execute(
            "SELECT dataset_id FROM datasets WHERE data_source_label IN "
            "(SELECT unnest($1::VARCHAR[]))",
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
    db_conn.register("_stage_files", df)
    try:
        db_conn.execute(sql_path.read_text(encoding="utf-8"))
        db_conn.commit()
    finally:
        db_conn.unregister("_stage_files")
    logger.success(f"Ingested {len(df):,} files successfully.")


def ingest_publications(
    df: pd.DataFrame,
    db_conn: duckdb.DuckDBPyConnection,
    sql_path: Path = Path("params/queries/ingest_publications.sql"),
    logger: "loguru.Logger" = loguru.logger,
) -> None:
    """Ingest publications metadata into DuckDB."""
    if df.empty:
        logger.warning("No publications to ingest, skipping.")
        return

    with httpx.Client(timeout=10.0) as client:
        authors_df, models_df, links_df, pub_datasets_df, resolved_datasets_df = (
            extract_publications_relational_data(df, client, logger)
        )

    stage_publications = df.drop(
        columns=[
            "authors",
            "linked_models",
            "model_ids",
            "model_references",
            "materials_and_methods",
            "methods_section",
            "dataset_references",
        ],
        errors="ignore",
    )

    views = {
        "_stage_publications": stage_publications,
        "_stage_publications_authors": authors_df,
        "_stage_ai_models": models_df,
        "_stage_publications_models_link": links_df,
        "_stage_publications_datasets_link": pub_datasets_df,
        "_stage_resolved_datasets": resolved_datasets_df,
    }

    for name, view_df in views.items():
        db_conn.register(name, view_df)
    try:
        db_conn.execute(sql_path.read_text(encoding="utf-8"))
        db_conn.commit()
    finally:
        for name in views:
            db_conn.unregister(name)
    logger.success(f"Ingested {len(df):,} publications successfully.")


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
    elif data_type == "publications":
        ingest_publications(load_publications_df(str(parquet_path)), db, logger=logger)

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
    type=click.Choice(["datasets", "files", "publications"], case_sensitive=False),
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
