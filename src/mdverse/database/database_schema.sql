-- ================================================================
-- database_schema.sql
-- MDverse database schema — single source of truth.
--
-- ALL CREATE TABLE and CREATE INDEX statements live here.
-- No SQL is defined anywhere else.
--
-- This file is read and executed by create_database.py.
-- It can also be executed directly from the command line:
--     sqlite3 database.db < database_schema.sql
-- ================================================================

PRAGMA foreign_keys = ON;

-- ── Lookup / Type Tables ──────────────────────────────────────
-- No foreign keys — created first, safe to reference immediately.

CREATE TABLE IF NOT EXISTS file_types (
    file_type_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL UNIQUE,
    comment       TEXT
);

CREATE TABLE IF NOT EXISTS molecule_types (
    molecule_type_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    name              TEXT    NOT NULL UNIQUE,
    comment           TEXT
);

CREATE TABLE IF NOT EXISTS databases (
    database_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT    NOT NULL UNIQUE,
    url          TEXT,
    comment      TEXT
);

CREATE TABLE IF NOT EXISTS data_sources (
    data_source_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL UNIQUE,
    url             TEXT,
    citation        TEXT,
    comment         TEXT
);

CREATE TABLE IF NOT EXISTS provenance_types (
    provenance_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT    NOT NULL UNIQUE,
    comment        TEXT
);

CREATE TABLE IF NOT EXISTS annotation_types (
    annotation_type_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    label               TEXT    NOT NULL,
    name                TEXT    NOT NULL,
    comment             TEXT,
    UNIQUE (name, label)
);

-- ── Main Tables ───────────────────────────────────────────────
-- Core scientific entities. Created before their dependents.

CREATE TABLE IF NOT EXISTS authors (
    author_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL,
    orcid      TEXT,
    UNIQUE (name, orcid)
);

CREATE TABLE IF NOT EXISTS papers (
    paper_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    doi       TEXT,
    title     TEXT    NOT NULL,
    abstract  TEXT,
    journal   TEXT    NOT NULL,
    url       TEXT,
    year      TEXT,
    keywords  TEXT
);

CREATE TABLE IF NOT EXISTS projects (
    project_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE,
    url         TEXT    NOT NULL,
    comment     TEXT,
    citation    TEXT
);

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
);

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
);

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
);

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
);

-- ── Simulation File Tables ────────────────────────────────────
-- file_id is both PK and FK — enforces one metadata record per file.

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
);

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
);

CREATE TABLE IF NOT EXISTS trajectory_files (
    file_id       INTEGER PRIMARY KEY
                      REFERENCES files (file_id)
                      ON DELETE CASCADE,
    atom_number   INTEGER NOT NULL,
    frame_number  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS molecules_external_db (
    mol_ext_db_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    molecule_id         INTEGER NOT NULL
                            REFERENCES molecules (molecule_id),
    db_name             TEXT    NOT NULL,
    id_in_external_db   TEXT    NOT NULL,
    database_id         INTEGER
                            REFERENCES databases (database_id)
);

-- ── Indexes ───────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_files_is_from_zip_file
    ON files (is_from_zip_file);

CREATE INDEX IF NOT EXISTS idx_mol_ext_db_db_name
    ON molecules_external_db (db_name);

-- ── Many-to-Many Link Tables ──────────────────────────────────
-- Created LAST — both referenced tables must already exist.

CREATE TABLE IF NOT EXISTS datasets_authors_link (
    dataset_id  INTEGER NOT NULL
                    REFERENCES datasets (dataset_id),
    author_id   INTEGER NOT NULL
                    REFERENCES authors (author_id),
    PRIMARY KEY (dataset_id, author_id)
);

CREATE TABLE IF NOT EXISTS authors_papers_link (
    author_id  INTEGER NOT NULL
                   REFERENCES authors (author_id),
    paper_id   INTEGER NOT NULL
                   REFERENCES papers (paper_id),
    PRIMARY KEY (author_id, paper_id)
);
