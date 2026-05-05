"""
Create the MDverse database schema using Python's built-in sqlite3 module.

The database stores molecular dynamics (MD) simulation datasets collected from
scientific repositories (Zenodo, Figshare, ATLAS, NOMAD, GPCRmd, MDDB).

Terminology:
    DB  = Database
    FK  = Foreign Key   — column whose value must match a PK in another table
    PK  = Primary Key   — unique row identifier
    MtM = Many-to-Many relationship

Naming conventions:
    Tables   : snake_case, plural        (e.g. datasets, file_types)
    Columns  : snake_case                (e.g. dataset_id, file_type_id)
    Indexes  : idx_<table>_<column>
    MtM link tables : table1_table2_link (e.g. datasets_authors_link)

SQLite-specific notes:
    - FK syntax     : REFERENCES other_table (other_column) on the column
    - FK enforcement: OFF by default — conn.execute("PRAGMA foreign_keys = ON")
                      must be called on every connection
    - Booleans      : stored as INTEGER (0 = False, 1 = True)
    - Dates         : stored as TEXT in ISO 8601 format (YYYY-MM-DD or
                      YYYY-MM-DDTHH:MM:SS) — no native date type in SQLite

Table creation order (parent tables must exist before child tables):
    (1) Lookup / type tables   — no FKs, created first
    (2) Independent main tables — Author, Paper, Project
    (3) Core main tables        — Dataset, File, Annotation, Molecule,
                                   MoleculeExternalDb
    (4) Simulation file tables  — TopologyFile, ParameterFile, TrajectoryFile
    (5) MtM link tables         — created LAST, both sides must exist first

ON DELETE CASCADE summary:
    files.dataset_id          CASCADE   topology_files.file_id    CASCADE
    annotations.file_id       CASCADE   parameter_files.file_id   CASCADE
    molecules.annotation_id   CASCADE   trajectory_files.file_id  CASCADE
    annotations.dataset_id    no cascade (intentional — requires manual review)
"""

import sys
import sqlite3
import argparse
import time
from datetime import timedelta
from pathlib import Path


# ============================================================================
# Configuration
# ============================================================================

DB_PATH = Path(__file__).parent / "database.db"


# ============================================================================
# Schema — ordered by dependency (parents before children)
# Each entry: (label, sql) — label used for logging and --dry-run output.
# ============================================================================

SCHEMA_STATEMENTS: list[tuple[str, str]] = [

    # ==========================================================================
    # Lookup / "Type" Tables
    # No FKs — safe to create first. One-to-many with the tables that use them.
    # ==========================================================================

    # File extension/format registry (e.g. 'gro', 'mdp', 'xtc', 'zip').
    # Relationships: files
    ("FileType", """
        CREATE TABLE IF NOT EXISTS file_types (
            file_type_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            name          TEXT    NOT NULL UNIQUE,
            comment       TEXT
        )
    """),

    # Molecule category registry (e.g. 'protein', 'lipid', 'nucleic acid').
    # Relationships: molecules
    ("MoleculeType", """
        CREATE TABLE IF NOT EXISTS molecule_types (
            molecule_type_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            name              TEXT    NOT NULL UNIQUE,
            comment           TEXT
        )
    """),

    # External scientific database registry (e.g. 'UniProt', 'PDB', 'ChEMBL').
    # Relationships: molecules_external_db
    ("Database", """
        CREATE TABLE IF NOT EXISTS databases (
            database_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT    NOT NULL UNIQUE,
            url          TEXT,
            comment      TEXT
        )
    """),

    # Source repository registry (e.g. 'zenodo', 'figshare', 'nomad').
    # Each dataset belongs to exactly one data source (one-to-many).
    # Relationships: datasets
    ("DataSource", """
        CREATE TABLE IF NOT EXISTS data_sources (
            data_source_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT    NOT NULL UNIQUE,
            url             TEXT,
            citation        TEXT,
            comment         TEXT
        )
    """),

    # Annotation provenance registry (e.g. 'manual', 'automatic', 'computed').
    # NOTE: PK is provenance_id, not provenance_type_id — intentional.
    # Relationships: annotations
    ("ProvenanceType", """
        CREATE TABLE IF NOT EXISTS provenance_types (
            provenance_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            name           TEXT    NOT NULL UNIQUE,
            comment        TEXT
        )
    """),

    # Annotation category registry. UNIQUE (name, label) — the pair must be
    # unique, allowing each field to repeat across different pairs.
    # Relationships: annotations
    ("AnnotationType", """
        CREATE TABLE IF NOT EXISTS annotation_types (
            annotation_type_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            label               TEXT    NOT NULL,
            name                TEXT    NOT NULL,
            comment             TEXT,
            UNIQUE (name, label)
        )
    """),

    # ==========================================================================
    # Main Tables
    # Core scientific entities. Author, Paper, Project have no FKs to other
    # main tables and are created before Dataset.
    # ==========================================================================

    # Unique researcher identities.
    # UNIQUE (name, orcid): SQLite NULL quirk — two rows with the same name
    # and orcid=NULL are both allowed (NULL is distinct from NULL in SQLite).
    # Relationships: datasets (via MtM), papers (via MtM)
    ("Author", """
        CREATE TABLE IF NOT EXISTS authors (
            author_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT    NOT NULL,
            orcid      TEXT,
            UNIQUE (name, orcid)
        )
    """),

    # Scientific publications. Authors linked via authors_papers_link (MtM).
    # Relationships: authors (via MtM), annotations
    ("Paper", """
        CREATE TABLE IF NOT EXISTS papers (
            paper_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            doi       TEXT,
            title     TEXT    NOT NULL,
            abstract  TEXT,
            journal   TEXT    NOT NULL,
            url       TEXT,
            year      TEXT,
            keywords  TEXT
        )
    """),

    # Research projects grouping multiple datasets.
    # Relationships: datasets
    ("Project", """
        CREATE TABLE IF NOT EXISTS projects (
            project_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL UNIQUE,
            url         TEXT    NOT NULL,
            comment     TEXT,
            citation    TEXT
        )
    """),

    # Central table — every collected dataset becomes one row here.
    # A dataset can have many files, authors, and annotations, but belongs
    # to exactly one data source.
    # Relationships: files, data_sources, authors (via MtM), projects,
    #                annotations
    ("Dataset", """
        CREATE TABLE IF NOT EXISTS datasets (
            dataset_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            data_source_id      INTEGER NOT NULL
                                    REFERENCES data_sources (data_source_id),
            id_in_data_source   TEXT    NOT NULL,
            url_in_data_source  TEXT,
            project_id          INTEGER
                                    REFERENCES projects (project_id),
            id_in_project       TEXT,
            url_in_project      TEXT,
            doi                 TEXT,
            date_created        TEXT,
            date_last_modified  TEXT,
            date_last_crawled   TEXT    NOT NULL,
            file_number         INTEGER NOT NULL DEFAULT 0,
            download_number     INTEGER NOT NULL DEFAULT 0,
            view_number         INTEGER NOT NULL DEFAULT 0,
            license             TEXT,
            title               TEXT    NOT NULL,
            description         TEXT,
            keywords            TEXT
        )
    """),

    # All files across all datasets. Top-level files and zip-extracted files
    # are both stored here, distinguished by is_from_zip_file.
    # Self-referencing FK: parent_zip_file_id → files (file_id).
    # md5 and url are NULL for files extracted from zip archives.
    # Relationships: datasets, files (self-ref), topology_files,
    #                parameter_files, trajectory_files, file_types, annotations
    ("File", """
        CREATE TABLE IF NOT EXISTS files (
            file_id             INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_id          INTEGER NOT NULL
                                    REFERENCES datasets (dataset_id)
                                    ON DELETE CASCADE,
            name                TEXT    NOT NULL,
            file_type_id        INTEGER NOT NULL
                                    REFERENCES file_types (file_type_id),
            size_in_bytes       REAL,
            md5                 TEXT,
            url                 TEXT,
            is_from_zip_file    INTEGER NOT NULL,
            parent_zip_file_id  INTEGER
                                    REFERENCES files (file_id)
        )
    """),

    ("idx_files_is_from_zip_file", """
        CREATE INDEX IF NOT EXISTS idx_files_is_from_zip_file
            ON files (is_from_zip_file)
    """),

    # Scientific labels attached to datasets or files (e.g. molecule name,
    # force field, software). file_id cascades; dataset_id does not
    # (dataset-level annotations require manual review before deletion).
    # Relationships: datasets, provenance_types, annotation_types, files, papers
    ("Annotation", """
        CREATE TABLE IF NOT EXISTS annotations (
            annotation_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_id          INTEGER NOT NULL
                                    REFERENCES datasets (dataset_id),
            provenance_type_id  INTEGER NOT NULL
                                    REFERENCES provenance_types (provenance_id),
            annotation_type_id  INTEGER NOT NULL
                                    REFERENCES annotation_types (annotation_type_id),
            file_id             INTEGER
                                    REFERENCES files (file_id)
                                    ON DELETE CASCADE,
            paper_id            INTEGER
                                    REFERENCES papers (paper_id),
            value               TEXT    NOT NULL,
            quality_score       TEXT,
            value_extra         TEXT,
            comment             TEXT
        )
    """),

    # Molecular entities found in simulation datasets, linked via annotations.
    # Relationships: annotations, molecule_types, molecules_external_db
    ("Molecule", """
        CREATE TABLE IF NOT EXISTS molecules (
            molecule_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            annotation_id     INTEGER NOT NULL
                                  REFERENCES annotations (annotation_id)
                                  ON DELETE CASCADE,
            name              TEXT    NOT NULL,
            formula           TEXT    NOT NULL,
            sequence          TEXT    NOT NULL,
            molecule_type_id  INTEGER
                                  REFERENCES molecule_types (molecule_type_id)
        )
    """),

    # ==========================================================================
    # Simulation File Tables — one-to-one with files
    # file_id is both PK and FK (no AUTOINCREMENT). Enforces exactly one
    # metadata record per file.
    # ==========================================================================

    # Metadata for .gro topology files. Boolean flags enable fast queries
    # (e.g. "find all datasets containing a protein and a lipid").
    # Relationships: files
    ("TopologyFile", """
        CREATE TABLE IF NOT EXISTS topology_files (
            file_id        INTEGER PRIMARY KEY
                               REFERENCES files (file_id)
                               ON DELETE CASCADE,
            atom_number    INTEGER NOT NULL,
            has_protein    INTEGER NOT NULL,
            has_nucleic    INTEGER NOT NULL,
            has_lipid      INTEGER NOT NULL,
            has_glucid     INTEGER NOT NULL,
            has_water_ion  INTEGER NOT NULL
        )
    """),

    # Metadata for .mdp GROMACS parameter files. All columns nullable —
    # not every .mdp file can be fully parsed.
    # Relationships: files
    ("ParameterFile", """
        CREATE TABLE IF NOT EXISTS parameter_files (
            file_id      INTEGER PRIMARY KEY
                             REFERENCES files (file_id)
                             ON DELETE CASCADE,
            dt           REAL,
            nsteps       INTEGER,
            temperature  REAL,
            thermostat   TEXT,
            barostat     TEXT,
            integrator   TEXT
        )
    """),

    # Metadata for .xtc trajectory files.
    # Relationships: files
    ("TrajectoryFile", """
        CREATE TABLE IF NOT EXISTS trajectory_files (
            file_id       INTEGER PRIMARY KEY
                              REFERENCES files (file_id)
                              ON DELETE CASCADE,
            atom_number   INTEGER NOT NULL,
            frame_number  INTEGER NOT NULL
        )
    """),

    # Links molecules to entries in external databases (e.g. UniProt P00698).
    # Relationships: molecules, databases
    ("MoleculeExternalDb", """
        CREATE TABLE IF NOT EXISTS molecules_external_db (
            mol_ext_db_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            molecule_id         INTEGER NOT NULL
                                    REFERENCES molecules (molecule_id),
            db_name             TEXT    NOT NULL,
            id_in_external_db   TEXT    NOT NULL,
            database_id         INTEGER
                                    REFERENCES databases (database_id)
        )
    """),

    ("idx_mol_ext_db_db_name", """
        CREATE INDEX IF NOT EXISTS idx_mol_ext_db_db_name
            ON molecules_external_db (db_name)
    """),

    # ==========================================================================
    # Many-to-Many (MtM) Link Tables
    # Created LAST — both referenced tables must exist before these can
    # reference them. Composite PK prevents duplicate pairs. No AUTOINCREMENT.
    # Also known as: junction table, join table, association table.
    # ==========================================================================

    # MtM: a dataset can have many authors; an author can publish many datasets.
    # Relationships: datasets, authors
    ("DatasetAuthorLink", """
        CREATE TABLE IF NOT EXISTS datasets_authors_link (
            dataset_id  INTEGER NOT NULL
                            REFERENCES datasets (dataset_id),
            author_id   INTEGER NOT NULL
                            REFERENCES authors (author_id),
            PRIMARY KEY (dataset_id, author_id)
        )
    """),

    # MtM: an author can write many papers; a paper is written by many authors.
    # Relationships: authors, papers
    ("AuthorPaperLink", """
        CREATE TABLE IF NOT EXISTS authors_papers_link (
            author_id  INTEGER NOT NULL
                           REFERENCES authors (author_id),
            paper_id   INTEGER NOT NULL
                           REFERENCES papers (paper_id),
            PRIMARY KEY (author_id, paper_id)
        )
    """),
]


# ============================================================================
# Helpers
# ============================================================================

def get_connection(db_path: Path) -> sqlite3.Connection:
    """Open a connection and enable FK enforcement.
    PRAGMA foreign_keys is not persisted — must be set on every connection."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ============================================================================
# Core logic
# ============================================================================

def create_schema(db_path: Path, dry_run: bool = False) -> None:
    """Execute all CREATE TABLE / INDEX statements in a single transaction.
    Idempotent — IF NOT EXISTS makes it safe to re-run on an existing database.
    In dry-run mode prints SQL to stdout without touching the database file."""
    if dry_run:
        for label, sql in SCHEMA_STATEMENTS:
            print(f"-- {label} --\n{sql.strip()}\n")
        return

    conn = get_connection(db_path)
    try:
        with conn:  # commits on success, rolls back automatically on any exception
            for label, sql in SCHEMA_STATEMENTS:
                conn.execute(sql.strip())
                print(f"  {label}... OK")
        print(f"OK | Database: {db_path.resolve()}")
    except sqlite3.Error as exc:
        print(f"ERROR | {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


# ============================================================================
# CLI
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Create the MDverse database schema.")
    parser.add_argument("--db", metavar="PATH", default=str(DB_PATH),
                        help=f"Database path (default: {DB_PATH}).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print SQL without creating the database.")
    args = parser.parse_args()
    start = time.perf_counter()
    create_schema(Path(args.db), dry_run=args.dry_run)
    print(f"Done in {str(timedelta(seconds=time.perf_counter() - start)).split('.')[0]}s")


if __name__ == "__main__":
    main()