"""Report row/column counts for MDverse DuckDB tables and generate ERD diagram."""

import sys
from pathlib import Path

import click
import duckdb
from loguru import logger

from mdverse.core.logger import create_logger

TABLES = [
    "data_sources",
    "datasets",
    "datasets_authors_link",
    "authors",
    "files",
    "file_types",
    "projects",
    "papers",
    "authors_papers_link",
    "annotations",
    "annotation_types",
    "provenance_types",
    "molecules",
    "molecule_types",
    "molecules_external_db",
    "databases",
]

RELATIONSHIPS = {
    # ||--o| (One-to-Zero-or-One)
    ("files", "files"): "||--o|",
    ("molecule_types", "molecules"): "||--o|",
    ("databases", "molecules_external_databases"): "||--o|",
    # ||--o{ (One-to-Zero-or-More)
    ("projects", "datasets"): "||--o{",
    ("persons", "authors_papers_link"): "||--o{",
    ("datasets", "datasets_authors_link"): "||--o{",
    # ||--|{ (One-to-One-or-More)
    ("data_sources", "datasets"): "||--|{",
    ("persons", "datasets_authors_link"): "||--|{",
    ("papers", "authors_papers_link"): "||--|{",
    ("file_types", "files"): "||--|{",
    ("datasets", "files"): "||--|{",
    ("annotation_categories", "annotations"): "||--|{",
    ("annotation_provenances", "annotations"): "||--|{",
    # }o--|| (Many-to-One)
    ("molecules", "annotations"): "}o--||",
    ("molecules_external_databases", "molecules"): "}o--||",
    # }o--o| (Many-to-Zero-or-One)
    ("annotations", "files"): "}o--o|",
    ("annotations", "datasets"): "}o--o|",
    ("annotations", "papers"): "}o--o|",
}

# URL of the DuckDB Mermaid ERD Macro
MACRO_URL = "https://gist.github.com/lmangani/dc9ea2ba0a0b2a54a1330e7db868e0bc/raw/297bbabdb588b4917cf7a357194cc0558bfcb5e9/mermaid.sql"


def generate_schema_diagram(conn: duckdb.DuckDBPyConnection, output_path: Path) -> None:
    """Generate a Markdown ERD file using DuckDB SQL Macro with custom relationship."""
    logger.info("Generating ERD diagram.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        # Load the webmacro extension and load the ERD macro from URL
        conn.execute("INSTALL webmacro FROM community; LOAD webmacro;")
        conn.execute(f"SELECT load_macro_from_url('{MACRO_URL}');")
        # Execute macro to extract database relationships
        result = conn.execute("""
            SELECT line
            FROM generate_er_diagram('er-diagram.mermaid', '%', '||--o{')
            ORDER BY line_num
        """).fetchall()
    except duckdb.Error as e:
        logger.error(f"DuckDB error generating ERD diagram: {e}")

    # Dynamically replace default relationship symbols in Mermaid output
    refined_lines = []
    for row in result:
        line = row[0]
        # Line format example: "data_sources ||--o{ datasets : "data_source_label""
        if "--" in line and ":" in line:
            parts = line.split()
            if len(parts) >= 3:
                table_a, _, table_b = parts[0], parts[1], parts[2]
                # Update line if relationship pair exists in lookup dict
                if (table_a, table_b) in RELATIONSHIPS:
                    new_symbol = RELATIONSHIPS[table_a, table_b]
                    line = (
                        f"    {table_a} {new_symbol} {table_b} :{line.split(':', 1)[1]}"
                    )
        refined_lines.append(line)

    # Format as Markdown code block and save to .md file
    markdown_content = "```mermaid\n" + "\n".join(refined_lines) + "\n```\n"
    md_file = output_path.with_suffix(".md")
    md_file.write_text(markdown_content, encoding="utf-8")
    logger.success(f"Saved DB schema documentation to: {md_file}")


def print_report(conn: duckdb.DuckDBPyConnection, db_path: Path) -> None:
    """Log table row and column metrics."""
    col_counts = {
        row[0]: row[1]
        for row in conn.execute("""
            SELECT table_name, COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = 'main'
            GROUP BY table_name
        """).fetchall()
    }
    logger.info("MDverse Database Report")
    logger.info(f"Timestamp: {conn.execute('SELECT CURRENT_TIMESTAMP').fetchone()[0]}")
    logger.info(f"Path: {db_path}")
    logger.info("-" * 62)
    logger.info(f"{'Table':<28} {'Columns':>7}  {'Rows':>12}")
    logger.info("-" * 62)
    total = 0
    for table in TABLES:
        try:
            n_rows = conn.table(table).count("*").fetchone()[0]
            total += n_rows
            logger.info(f"{table:<28} {col_counts.get(table, 0):>7,}  {n_rows:>12,}")
        except duckdb.CatalogException:
            logger.warning(f"{table:<28} {'MISSING':>7}  {'N/A':>12}")

    logger.info("-" * 62)
    logger.info(f"{'TOTAL ROWS':<28} {'':>7}  {total:>12,}")


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
    """Report row/column counts for MDverse tables and generate schema docs."""
    logger = create_logger()
    if not db_path.exists():
        logger.warning(f"Database file not found: {db_path}")
        logger.error("Exiting.")
        sys.exit(1)
    # Database connection opened in read/write context if loading macro in-memory
    conn = duckdb.connect(str(db_path), read_only=False)
    try:
        print_report(conn, db_path)
        # Generate schema diagram if output path is provided
        if schema_outpath:
            generate_schema_diagram(conn, schema_outpath)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
