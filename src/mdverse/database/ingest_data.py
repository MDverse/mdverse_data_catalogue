"""Ingest scraped data into the MDverse database."""

import time
from datetime import timedelta
from pathlib import Path

import click
import duckdb
import loguru
import numpy as np
import pandas as pd

from mdverse.core.logger import create_logger
from mdverse.models.enums import DatasetSourceName

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
    ("simulation_timesteps_in_fs", "SIMULATION_TIMESTEP"),
    ("simulation_times", "SIMULATION_TIME"),
    ("simulation_temperatures_in_kelvin", "SIMULATION_TEMPERATURE"),
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


def init_db_connection(
    db_path: Path, *, read_only: bool = False, logger: "loguru.Logger" = loguru.logger
) -> duckdb.DuckDBPyConnection:
    """Initialize a connection to the DuckDB database.

    Returns
    -------
    duckdb.DuckDBPyConnection
        A connection object to interact with the DuckDB database.

    Raises
    ------
    PermissionError
        If read or write permissions are missing for the database file or directory.
    duckdb.IOException
        If an I/O error or file lock contention occurs while opening the database.
    duckdb.CatalogException
        If there is a catalog resolution or database structural issue.
    duckdb.Error
        If DuckDB encounters a general error opening or initializing the database.
    """
    try:
        db = duckdb.connect(str(db_path), read_only=read_only)
        logger.success(f"Successfully connected to DuckDB database at {db_path}.")
        return db

    except PermissionError as e:
        logger.error(f"Permission denied accessing DuckDB database at {db_path}: {e}")
        raise
    except (duckdb.IOException, duckdb.CatalogException) as e:
        logger.error(f"DuckDB I/O or catalog error opening {db_path}: {e}")
        raise
    except duckdb.Error as e:
        logger.error(f"Failed to connect to DuckDB database at {db_path}: {e}")
        raise


def resolve_source_attribute(src_name: str, attr: str) -> str | None:
    """Resolve a source attribute from the DatasetSourceName enum.

    Returns
    -------
    str | None
        The attribute value if the source name is valid, otherwise None.
    """
    if src_name in DatasetSourceName._value2member_map_:
        return getattr(DatasetSourceName(src_name), attr)
    return None


def load_datasets_df(parquet_path: str) -> pd.DataFrame:
    """Load parquet file into a Pandas dataframe for datasets metadata.

    Returns
    -------
    pd.DataFrame
        A dataframe containing the datasets metadata.
    """
    df = pd.read_parquet(parquet_path)
    df = df.rename(columns=DATASETS_COLUMN_MAPPING)
    # Flatten the keywords list into a single string for database storage.
    df["keywords"] = df["keywords"].apply(
        lambda row: " ;".join(row) if isinstance(row, (list, tuple, np.ndarray)) else ""
    )
    # Convert numeric columns to integers, handling NaN values appropriately.
    for col in ("file_number", "download_number", "view_number"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    # Attribute resolution for data source URL, citation, and comment.
    df["data_source_url"] = df["data_source_label"].apply(
        lambda src: resolve_source_attribute(src, "url")
    )
    df["data_source_citation"] = df["data_source_label"].apply(
        lambda src: resolve_source_attribute(src, "citation")
    )
    df["data_source_comment"] = df["data_source_label"].apply(
        lambda src: resolve_source_attribute(src, "comment")
    )
    return df


def load_files_df(parquet_path: str) -> pd.DataFrame:
    """Load parquet file into a Pandas dataframe for files metadata.

    Returns
    -------
    pd.DataFrame
        A dataframe containing the files metadata.
    """
    df = pd.read_parquet(
        parquet_path,
        columns=list(FILES_COLUMN_MAPPING.keys()),
    )
    df = df.rename(columns=FILES_COLUMN_MAPPING)
    # Derive boolean/flag for archive containment.
    df["is_from_zip_file"] = df["parent_zip_file_name"].notna().astype(int)
    # Convert numeric columns to integers, handling NaN values appropriately.
    if "size_in_bytes" in df.columns:
        df["size_in_bytes"] = pd.to_numeric(
            df["size_in_bytes"], errors="coerce"
        ).astype("Int64")

    return df


def _extract_author_records(df: pd.DataFrame) -> list[dict[str, str]]:
    """Extract and normalize individual author records from datasets frame.

    Returns
    -------
    list[dict[str, str]]
        A list of dictionaries containing author records with dataset linkage.
    """
    records = []
    cols = ["id_in_data_source", "data_source_label", "authors"]

    seen_names = set()
    seen_orcids = set()

    for row in df[cols].dropna(subset=["authors"]).to_dict("records"):
        authors = row.get("authors")
        if authors is None:
            continue

        for author in authors:
            author_dict = (
                author.model_dump() if hasattr(author, "model_dump") else author
            )
            if not isinstance(author_dict, dict):
                continue

            full_name = author_dict.get("full_name")
            orcid = author_dict.get("orcid")
            orcid = str(orcid).strip() if orcid else None

            if not full_name or full_name in seen_names:
                continue
            if orcid and orcid in seen_orcids:
                continue

            seen_names.add(full_name)
            if orcid:
                seen_orcids.add(orcid)

            records.append(
                {
                    "data_source_label": row["data_source_label"],
                    "id_in_data_source": row["id_in_data_source"],
                    "full_name": full_name,
                    "orcid": orcid,
                    "first_name": author_dict.get("first_name"),
                    "last_name": author_dict.get("last_name"),
                    "affiliation": author_dict.get("affiliation"),
                }
            )

    expected_cols = [
        "data_source_label",
        "id_in_data_source",
        "full_name",
        "orcid",
        "first_name",
        "last_name",
        "affiliation",
    ]
    return pd.DataFrame(records, columns=expected_cols)


def _extract_paper_records(df: pd.DataFrame) -> list[dict[str, str]]:
    """Extract external publication links from datasets frame.

    Returns
    -------
    list[dict[str, str]]
        A list of dictionaries containing paper records with dataset linkage.
    """
    records = []
    cols = ["id_in_data_source", "data_source_label", "external_links"]

    for row in df[cols].dropna(subset=["external_links"]).to_dict("records"):
        links = row.get("external_links")
        if links is None:
            continue

        for link in links:
            if link:
                records.append(
                    {
                        "data_source_label": row["data_source_label"],
                        "id_in_data_source": row["id_in_data_source"],
                        "doi_or_url": str(link).strip(),
                    }
                )

    expected_cols = ["data_source_label", "id_in_data_source", "doi_or_url"]
    return pd.DataFrame(records, columns=expected_cols)


def _extract_simulation_records(df: pd.DataFrame) -> list[dict[str, str]]:
    """Extract software, forcefields, timesteps, times, temperatures per dataset.

    Returns
    -------
    list[dict[str, str]]
        A list of dictionaries containing simulation records with dataset linkage.
    """
    records = []
    cols = ["id_in_data_source", "data_source_label", "simulation"]

    # Iterate over dict records directly (significantly faster than iterrows)
    for row in df[cols].dropna(subset=["simulation"]).to_dict("records"):
        sim = row["simulation"]
        sim = sim.model_dump() if hasattr(sim, "model_dump") else sim
        if not isinstance(sim, dict):
            continue

        base = {
            "id_in_data_source": row["id_in_data_source"],
            "data_source_label": row["data_source_label"],
        }
        # Process named entity items (Software & Forcefields)
        for key, category in (
            ("software", "SOFTWARE"),
            ("forcefields_models", "FORCEFIELD_MODEL"),
        ):
            items = sim.get(key)
            if items is None:
                continue
            for item in items:
                if isinstance(item, dict) and (name := item.get("name")):
                    version = item.get("version")
                    val = f"{name} {version}".strip() if version else name
                    records.append({**base, "category_label": category, "value": val})
        # Process scalar lists via mapping configuration
        for key, category in SIMULATION_CATEGORY_MAPPING:
            values = sim.get(key)
            if values is None:
                continue
            for val in values:
                records.append({**base, "category_label": category, "value": str(val)})
    expected_cols = [
        "id_in_data_source",
        "data_source_label",
        "category_label",
        "value",
    ]
    return pd.DataFrame(records, columns=expected_cols)


def ingest_datasets(
    df: pd.DataFrame,
    db_conn: duckdb.DuckDBPyConnection,
    logger: "loguru.Logger" = loguru.logger,
) -> list[int]:
    """Ingest datasets from the given DataFrame into the database.

    Returns
    -------
    list[int]
        List of dataset_ids for all datasets in the source, after ingestion.
    """
    logger.info("Starting ingestion of datasets into the database.")
    # Extract staging records using helper functions (keeps complexity low)
    author_records = _extract_author_records(df)
    paper_records = _extract_paper_records(df)
    simulation_records = _extract_simulation_records(df)
    # Register temporary staging views in DuckDB
    db_conn.register(
        "_stage_datasets",
        df.drop(columns=["authors", "external_links"], errors="ignore"),
    )
    db_conn.register("_stage_authors", pd.DataFrame(author_records))
    db_conn.register("_stage_papers", pd.DataFrame(paper_records))
    db_conn.register("_stage_simulations", pd.DataFrame(simulation_records))
    # Upsert parent metadata entities
    db_conn.execute("""
        INSERT INTO data_sources (data_source_label, url, citation, comment)
        SELECT
            data_source_label,
            FIRST(data_source_url),
            FIRST(data_source_citation),
            FIRST(data_source_comment)
        FROM _stage_datasets
        WHERE data_source_label IS NOT NULL
          AND data_source_label NOT IN (
            SELECT data_source_label FROM data_sources WHERE data_source_label IS NOT NULL
        )
        GROUP BY data_source_label;

        INSERT INTO projects (project_label, url)
        SELECT
            project_label,
            FIRST(url_in_project)
        FROM _stage_datasets
        WHERE project_label IS NOT NULL
          AND project_label NOT IN (
            SELECT project_label FROM projects WHERE project_label IS NOT NULL
        )
        GROUP BY project_label;

        INSERT INTO persons (full_name, orcid, first_name, last_name, affiliation)
        SELECT 
            full_name,
            orcid,
            first_name,
            last_name,
            affiliation
        FROM _stage_authors
        WHERE full_name NOT IN (SELECT full_name FROM persons WHERE full_name IS NOT NULL)
        AND (orcid IS NULL OR orcid NOT IN (SELECT orcid FROM persons WHERE orcid IS NOT NULL));

        INSERT INTO papers (doi, url, title)
        SELECT
            doi_or_url AS doi,
            doi_or_url AS url,
            doi_or_url AS title
        FROM _stage_papers
        WHERE doi_or_url IS NOT NULL
          AND doi_or_url NOT IN (
            SELECT doi FROM papers WHERE doi IS NOT NULL
        )
        GROUP BY doi_or_url;
    """)
    # Upsert datasets and update existing ones
    db_conn.execute("""
        INSERT INTO datasets (
            data_source_label, id_in_data_source, url_in_data_source,
            project_label, doi, date_created, date_last_updated,
            date_last_fetched, file_number, download_number, view_number,
            license, title, description, keywords
        )
        SELECT
            data_source_label,
            id_in_data_source,
            FIRST(url_in_data_source),
            FIRST(project_label),
            FIRST(doi),
            FIRST(date_created),
            FIRST(date_last_updated),
            FIRST(date_last_fetched),
            FIRST(file_number),
            FIRST(download_number),
            FIRST(view_number),
            FIRST(license),
            FIRST(title),
            FIRST(description),
            FIRST(keywords)
        FROM _stage_datasets
        GROUP BY data_source_label, id_in_data_source
        ON CONFLICT (data_source_label, id_in_data_source) DO NOTHING;
    """)
    # Update existing datasets
    db_conn.execute("""
        UPDATE datasets d
        SET
            doi                = s.doi,
            date_last_updated = s.date_last_updated,
            date_last_fetched  = s.date_last_fetched,
            file_number        = s.file_number,
            download_number    = s.download_number,
            view_number        = s.view_number,
            title              = s.title,
            description        = s.description,
            keywords           = s.keywords
        FROM _stage_datasets s
        WHERE d.data_source_label = s.data_source_label
          AND d.id_in_data_source = s.id_in_data_source;
    """)
    # Insert link between datasets and external paper publications
    db_conn.execute("""
        INSERT INTO datasets_papers_link (dataset_id, paper_id)
        SELECT DISTINCT d.dataset_id, p.paper_id
        FROM _stage_papers sp
        JOIN datasets d
          ON d.data_source_label = sp.data_source_label
         AND d.id_in_data_source = sp.id_in_data_source
        JOIN papers p
          ON p.doi = sp.doi_or_url OR p.url = sp.doi_or_url
        WHERE NOT EXISTS (
            SELECT 1 FROM datasets_papers_link dpl
            WHERE dpl.dataset_id = d.dataset_id
              AND dpl.paper_id = p.paper_id
        );
    """)
    db_conn.commit()
    # Clean up staging views
    for view in (
        "_stage_datasets",
        "_stage_authors",
        "_stage_papers",
        "_stage_simulations",
    ):
        db_conn.unregister(view)
    # Retrieve dataset_ids for downstream file pipeline
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


# ============================================================================
# Pipeline — files
#
# Two-pass SQL strategy handles parent-zip references without Python loops:
#   Pass 1 — insert all top-level files (is_from_zip_file = 0),
#             including zip archives themselves.
#   Pass 2 — insert zip-child files; parent_zip_file_id resolved via
#             a self-join on files already inserted in pass 1.
# ============================================================================


def _delete_files_for_datasets(
    conn: duckdb.DuckDBPyConnection,
    dataset_ids: list[int],
    logger: "loguru.Logger" = loguru.logger,
) -> None:
    """Delete all files and their simulation children for the given datasets."""
    if not dataset_ids:
        return
    # Remove files for the topology_files table.
    conn.execute(
        """
        DELETE FROM topology_files
        WHERE file_id IN (
            SELECT file_id FROM files
            WHERE dataset_id IN (SELECT unnest($1::INTEGER[]))
        )
        """,
        [dataset_ids],
    )
    # Remove files for the parameter_files table.
    conn.execute(
        """
        DELETE FROM parameter_files
        WHERE file_id IN (
            SELECT file_id FROM files
            WHERE dataset_id IN (SELECT unnest($1::INTEGER[]))
        )
        """,
        [dataset_ids],
    )
    # Remove files for the trajectory_files table.
    conn.execute(
        """
        DELETE FROM trajectory_files
        WHERE file_id IN (
            SELECT file_id FROM files
            WHERE dataset_id IN (SELECT unnest($1::INTEGER[]))
        )
        """,
        [dataset_ids],
    )
    conn.execute(
        "DELETE FROM files WHERE dataset_id IN (SELECT unnest($1::INTEGER[]))",
        [dataset_ids],
    )
    conn.commit()
    logger.info(f"Deleted existing files for {len(dataset_ids):,} dataset(s).")


def ingest_files(
    df: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
    dataset_ids: list[int],
    logger: "loguru.Logger" = loguru.logger,
) -> None:
    """Bulk-insert file rows for the given dataset_ids."""
    if not dataset_ids:
        logger.warning("No datasets to process — skipping file ingestion.")
        return

    # Resolve dataset_id in pandas (vectorised map) then filter to eligible only
    ds_map = {
        (r[0], r[1]): r[2]
        for r in conn.execute("""
            SELECT src.name, d.id_in_data_source, d.dataset_id
            FROM datasets d
            JOIN data_sources src ON src.data_source_id = d.data_source_id
        """).fetchall()
    }
    eligible = set(dataset_ids)
    df = df.copy()
    df["dataset_id"] = df.apply(
        lambda r: ds_map.get((r["data_source"], r["dataset_id_in_data_source"])),
        axis=1,
    )
    df = df[df["dataset_id"].isin(eligible)].reset_index(drop=True)

    if df.empty:
        logger.info("No matching datasets found in files parquet — nothing to ingest.")
        return

    conn.register("_stage_files", df)

    # ── Upsert file_types ──────────────────────────────────────────────────
    conn.execute("""
        INSERT INTO file_types (name)
        SELECT DISTINCT file_type_name
        FROM _stage_files
        WHERE file_type_name IS NOT NULL
          AND file_type_name NOT IN (SELECT name FROM file_types)
    """)

    # ── Pass 1: top-level files (not inside a zip) ─────────────────────────
    conn.execute("""
        INSERT INTO files (
            dataset_id, name, file_type_id, size_in_bytes,
            md5, url, is_from_zip_file, parent_zip_file_id
        )
        SELECT
            s.dataset_id,
            s.name,
            ft.file_type_id,
            s.size_in_bytes,
            s.md5,
            s.url,
            s.is_from_zip_file,
            NULL
        FROM _stage_files s
        JOIN file_types ft ON ft.name = s.file_type_name
        WHERE s.is_from_zip_file = 0
          AND NOT EXISTS (
              SELECT 1 FROM files f
              WHERE f.dataset_id = s.dataset_id AND f.name = s.name
          )
    """)

    # ── Pass 2: zip-child files — parent now exists from pass 1 ───────────
    conn.execute("""
        INSERT INTO files (
            dataset_id, name, file_type_id, size_in_bytes,
            md5, url, is_from_zip_file, parent_zip_file_id
        )
        SELECT
            s.dataset_id,
            s.name,
            ft.file_type_id,
            s.size_in_bytes,
            s.md5,
            s.url,
            s.is_from_zip_file,
            parent.file_id
        FROM _stage_files s
        JOIN file_types ft ON ft.name = s.file_type_name
        LEFT JOIN files parent
               ON parent.dataset_id = s.dataset_id
              AND parent.name       = s.parent_zip_file_name
        WHERE s.is_from_zip_file = 1
          AND NOT EXISTS (
              SELECT 1 FROM files f
              WHERE f.dataset_id = s.dataset_id AND f.name = s.name
          )
    """)

    conn.commit()
    conn.unregister("_stage_files")

    n = conn.execute(
        """
        SELECT COUNT(*) FROM files
        WHERE dataset_id IN (SELECT unnest($1::INTEGER[]))
    """,
        [dataset_ids],
    ).fetchone()[0]
    logger.success(f"Files ingested — {n:,} total rows for these datasets.")


def ingest(
    db_path: Path,
    data_type: str,
    parquet_path: Path,
    logger: "loguru.Logger" = loguru.logger,
) -> None:
    """Ingest data into the MDverse database."""
    logger.info(f"Starting ingestion into MDverse database: {db_path}.")
    logger.info(f"Data type: {data_type}.")
    logger.info(f"Source file: {parquet_path}.")
    mdverse_db = init_db_connection(db_path, read_only=False, logger=logger)
    if data_type == "datasets":
        ingest_datasets(load_datasets_df(str(parquet_path)), mdverse_db)

    elif data_type == "files":
        df = load_files_df(str(parquet_path))
        source_names = df["data_source"].unique().tolist()
        all_dataset_ids = [
            r[0]
            for r in mdverse_db.execute(
                """
                SELECT d.dataset_id
                FROM datasets d
                JOIN data_sources src ON src.data_source_id = d.data_source_id
                WHERE src.name IN (SELECT unnest($1::VARCHAR[]))
            """,
                [source_names],
            ).fetchall()
        ]

        ids_with_files = (
            {
                r[0]
                for r in mdverse_db.execute(
                    """
                SELECT DISTINCT dataset_id FROM files
                WHERE dataset_id IN (SELECT unnest($1::INTEGER[]))
            """,
                    [all_dataset_ids],
                ).fetchall()
            }
            if all_dataset_ids
            else set()
        )

        new_dataset_ids = [d for d in all_dataset_ids if d not in ids_with_files]

        if not new_dataset_ids:
            logger.info("All datasets already have files — nothing to ingest.")
        else:
            logger.info(f"{len(new_dataset_ids):,} dataset(s) need file ingestion.")
            _delete_files_for_datasets(mdverse_db, new_dataset_ids)
            ingest_files(df, mdverse_db, dataset_ids=new_dataset_ids)

    mdverse_db.close()


@click.command(
    help="Ingest data into the MDverse database.",
)
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True, file_okay=True, path_type=Path),
    required=True,
    help="Path to the DuckDB database file.",
)
@click.option(
    "--parquet",
    "parquet_path",
    type=click.Path(exists=True, file_okay=True, path_type=Path),
    required=True,
    help="Path to the parquet file.",
)
@click.option(
    "--type",
    "data_type",
    type=click.Choice(
        ["datasets", "files"],
        case_sensitive=False,
    ),
    required=True,
    help="Define data type to be ingested.",
)
def main(db_path: Path, parquet_path: Path, data_type: str) -> None:
    """Ingest data into the MDverse database."""
    logpath = Path("logs/ingest_data_into_database.log")
    logger = create_logger(logpath=logpath, level="INFO")
    start_time = time.perf_counter()
    ingest(db_path, data_type, parquet_path, logger=logger)
    elapsed_time = str(timedelta(seconds=time.perf_counter() - start_time)).split(".")[
        0
    ]
    logger.info(f"Total time: {elapsed_time}")
    logger.success("Done.")


if __name__ == "__main__":
    main()
