"""Report DuckDB metrics and generate Mermaid ERD documentation."""

import sys
from pathlib import Path

import click
import duckdb
from loguru import logger

from mdverse.core.logger import create_logger

TABLES = [
    "annotation_categories",
    "annotation_provenances",
    "annotations",
    "authors_papers_link",
    "data_sources",
    "databases",
    "datasets",
    "datasets_authors_link",
    "datasets_papers_link",
    "file_types",
    "files",
    "molecule_types",
    "molecules",
    "molecules_external_databases",
    "papers",
    "persons",
    "projects",
]

RELATIONSHIPS = {
    ("files", "files"): "||--o|",
    ("molecule_types", "molecules"): "||--o|",
    ("databases", "molecules_external_databases"): "||--o|",
    ("projects", "datasets"): "||--o{",
    ("persons", "authors_papers_link"): "||--o{",
    ("datasets", "datasets_authors_link"): "||--o{",
    ("data_sources", "datasets"): "||--|{",
    ("persons", "datasets_authors_link"): "||--|{",
    ("papers", "authors_papers_link"): "||--|{",
    ("file_types", "files"): "||--|{",
    ("datasets", "files"): "||--|{",
    ("annotation_categories", "annotations"): "||--|{",
    ("annotation_provenances", "annotations"): "||--|{",
    ("molecules", "annotations"): "}o--||",
    ("molecules_external_databases", "molecules"): "}o--||",
    ("annotations", "files"): "}o--o|",
    ("annotations", "datasets"): "}o--o|",
    ("annotations", "papers"): "}o--o|",
}

MACRO_URL = (
    "https://gist.github.com/lmangani/dc9ea2ba0a0b2a54a1330e7db868e0bc/raw/"
    "297bbabdb588b4917cf7a357194cc0558bfcb5e9/mermaid.sql"
)


def generate_schema_diagram(conn: duckdb.DuckDBPyConnection, out_path: Path) -> None:
    """Generate Markdown ERD diagram using DuckDB macro and custom symbols."""
    logger.info("Generating ERD diagram.")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn.execute("INSTALL webmacro FROM community; LOAD webmacro;")
        conn.execute(f"SELECT load_macro_from_url('{MACRO_URL}');")
        result = conn.execute("""
            SELECT line
            FROM generate_er_diagram('er-diagram.mermaid', '%', '||--o{')
            ORDER BY line_num
        """).fetchall()
    except duckdb.Error as e:
        logger.error(f"DuckDB ERD error: {e}")
        return

    # Replace default symbols with custom relationship mapping.
    lines = []
    for (raw_line,) in result:
        line = raw_line
        if "--" in line and ":" in line:
            parts = line.split()
            if len(parts) >= 3 and (parts[0], parts[2]) in RELATIONSHIPS:
                sym = RELATIONSHIPS[parts[0], parts[2]]
                lbl = line.split(":", 1)[1]
                line = f"    {parts[0]} {sym} {parts[2]} :{lbl}"
        lines.append(line)

    md_file = out_path.with_suffix(".md")
    md_file.write_text(f"```mermaid\n{'\n'.join(lines)}\n```\n", encoding="utf-8")
    logger.success(f"Saved DB schema documentation to: {md_file}")


def print_report(conn: duckdb.DuckDBPyConnection, db_path: Path) -> None:
    """Log column and row counts for all database tables."""
    cols = dict(
        conn.execute("""
            SELECT table_name, COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = 'main'
            GROUP BY table_name
        """).fetchall()
    )
    now = conn.execute("SELECT CURRENT_TIMESTAMP").fetchone()[0]
    logger.info("MDverse Database Report")
    logger.info(f"Timestamp: {now}")
    logger.info(f"Path: {db_path}")

    sep = "-" * 52
    logger.info(sep)
    logger.info(f"{'Table':<30} {'Columns':>8} {'Rows':>12}")
    logger.info(sep)

    total = 0
    for tbl in TABLES:
        try:
            rows = conn.table(tbl).count("*").fetchone()[0]
            total += rows
            logger.info(f"{tbl:<30} {cols.get(tbl, 0):>8,} {rows:>12,}")
        except duckdb.CatalogException:
            logger.warning(f"{tbl:<30} {'MISSING':>8} {'N/A':>12}")

    logger.info(sep)
    logger.info(f"{'TOTAL ROWS':<30} {'':>8} {total:>12,}")
    logger.info(sep)


def _format_date(val: object) -> str:
    """Format date value as YYYY-MM-DD.

    Returns
    -------
    str
        Formatted date string or "N/A" if value is None.
    """
    return str(val)[:10] if val else "N/A"


def print_data_sources_report(conn: duckdb.DuckDBPyConnection) -> None:
    """Log aggregated metrics grouped by dataset repository origin."""
    rows = conn.execute("""
        SELECT
            d.data_source_label,
            COUNT(DISTINCT d.dataset_id),
            MIN(d.date_created),
            MAX(d.date_created),
            COUNT(f.file_id),
            ROUND(
                COALESCE(SUM(f.size_in_bytes), 0) / (1024.0 * 1024.0 * 1024.0)
            )
        FROM datasets d
        LEFT JOIN files f ON f.dataset_id = d.dataset_id
        GROUP BY d.data_source_label
        ORDER BY d.data_source_label
    """).fetchall()

    hdr = (
        f"{'Dataset origin':<24} {'Number of datasets':>18} "
        f"{'First dataset':>13} {'Last dataset':>13} "
        f"{'Total files':>13} {'Total size (GB)':>15}"
    )
    sep = "-" * len(hdr)
    logger.info(sep)
    logger.info(hdr)
    logger.info(sep)

    tot_ds = tot_files = tot_size = 0
    min_date = max_date = None

    for origin, n_ds, first_ds, last_ds, n_files, size_gb in rows:
        tot_ds += n_ds
        tot_files += n_files
        tot_size += int(size_gb or 0)
        min_date = min(filter(None, [min_date, first_ds]), default=first_ds)
        max_date = max(filter(None, [max_date, last_ds]), default=last_ds)

        logger.info(
            f"{origin!s:<24} {n_ds:>18,} {_format_date(first_ds):>13} "
            f"{_format_date(last_ds):>13} {n_files:>13,} "
            f"{int(size_gb or 0):>15,}"
        )

    logger.info(sep)
    logger.info(
        f"{'Total':<24} {tot_ds:>18,} {_format_date(min_date):>13} "
        f"{_format_date(max_date):>13} {tot_files:>13,} {tot_size:>15,}"
    )
    logger.info(sep)


@click.command()
@click.option(
    "--db-path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
    help="Path to DuckDB database file.",
)
@click.option(
    "--schema-outpath",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Output path for schema documentation (e.g. docs/schema.md).",
)
def main(db_path: Path, schema_outpath: Path | None) -> None:
    """Report table metrics and generate schema documentation."""
    logger = create_logger("logs/report_db.log")
    if not db_path.exists():
        logger.error(f"Database file not found: {db_path}.")
        sys.exit(1)

    with duckdb.connect(str(db_path), read_only=False) as conn:
        print_report(conn, db_path)
        logger.info("")
        print_data_sources_report(conn)
        if schema_outpath:
            generate_schema_diagram(conn, schema_outpath)


if __name__ == "__main__":
    main()
