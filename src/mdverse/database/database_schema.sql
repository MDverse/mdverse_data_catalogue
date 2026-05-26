-- ================================================================
-- database_schema_duckdb.sql
-- MDverse database schema — single source of truth.
--
-- ALL CREATE TABLE and CREATE INDEX statements live here.
-- No SQL is defined anywhere else.
--
-- This file is read and executed by create_database_duckdb.py.
-- It can also be executed directly from the DuckDB CLI:
--     duckdb database.duckdb < database_schema_duckdb.sql
-- ================================================================

-- ── Lookup / Type Tables ──────────────────────────────────────
-- No foreign keys — created first, safe to reference immediately.

CREATE SEQUENCE IF NOT EXISTS file_types_seq START 1;
CREATE TABLE IF NOT EXISTS file_types (
    file_type_id  INTEGER PRIMARY KEY DEFAULT nextval('file_types_seq'),
    name          VARCHAR    NOT NULL UNIQUE,
    comment       VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS molecule_types_seq START 1;
CREATE TABLE IF NOT EXISTS molecule_types (
    molecule_type_id  INTEGER PRIMARY KEY DEFAULT nextval('molecule_types_seq'),
    name              VARCHAR    NOT NULL UNIQUE,
    comment           VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS databases_seq START 1;
CREATE TABLE IF NOT EXISTS databases (
    database_id  INTEGER PRIMARY KEY DEFAULT nextval('databases_seq'),
    name         VARCHAR    NOT NULL UNIQUE,
    url          VARCHAR,
    comment      VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS data_sources_seq START 1;
CREATE TABLE IF NOT EXISTS data_sources (
    data_source_id  INTEGER PRIMARY KEY DEFAULT nextval('data_sources_seq'),
    name            VARCHAR    NOT NULL UNIQUE,
    url             VARCHAR,
    citation        VARCHAR,
    comment         VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS provenance_types_seq START 1;
CREATE TABLE IF NOT EXISTS provenance_types (
    provenance_id  INTEGER PRIMARY KEY DEFAULT nextval('provenance_types_seq'),
    name           VARCHAR    NOT NULL UNIQUE,
    comment        VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS annotation_types_seq START 1;
CREATE TABLE IF NOT EXISTS annotation_types (
    annotation_type_id  INTEGER PRIMARY KEY DEFAULT nextval('annotation_types_seq'),
    label               VARCHAR    NOT NULL,
    name                VARCHAR    NOT NULL,
    comment             VARCHAR,
    UNIQUE (name, label)
);

-- ── Main Tables ───────────────────────────────────────────────

CREATE SEQUENCE IF NOT EXISTS authors_seq START 1;
CREATE TABLE IF NOT EXISTS authors (
    author_id  INTEGER PRIMARY KEY DEFAULT nextval('authors_seq'),
    name       VARCHAR    NOT NULL,
    orcid      VARCHAR,
    UNIQUE (name, orcid)
);

CREATE SEQUENCE IF NOT EXISTS papers_seq START 1;
CREATE TABLE IF NOT EXISTS papers (
    paper_id  INTEGER PRIMARY KEY DEFAULT nextval('papers_seq'),
    doi       VARCHAR,
    title     VARCHAR    NOT NULL,
    abstract  VARCHAR,
    journal   VARCHAR    NOT NULL,
    url       VARCHAR,
    year      VARCHAR,
    keywords  VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS projects_seq START 1;
CREATE TABLE IF NOT EXISTS projects (
    project_id  INTEGER PRIMARY KEY DEFAULT nextval('projects_seq'),
    name        VARCHAR    NOT NULL UNIQUE,
    url         VARCHAR    NOT NULL,
    comment     VARCHAR,
    citation    VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS datasets_seq START 1;
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id          INTEGER PRIMARY KEY DEFAULT nextval('datasets_seq'),
    data_source_id      INTEGER NOT NULL REFERENCES data_sources (data_source_id),
    id_in_data_source   VARCHAR    NOT NULL,
    url_in_data_source  VARCHAR,
    project_id          INTEGER REFERENCES projects (project_id),
    id_in_project       VARCHAR,
    url_in_project      VARCHAR,
    doi                 VARCHAR,
    date_created        TIMESTAMP,
    date_last_modified  TIMESTAMP,
    date_last_crawled   TIMESTAMP NOT NULL,
    file_number         INTEGER NOT NULL DEFAULT 0,
    download_number     INTEGER NOT NULL DEFAULT 0,
    view_number         INTEGER NOT NULL DEFAULT 0,
    license             VARCHAR,
    title               VARCHAR    NOT NULL,
    description         VARCHAR,
    keywords            VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS files_seq START 1;
CREATE TABLE IF NOT EXISTS files (
    file_id             INTEGER PRIMARY KEY DEFAULT nextval('files_seq'),
    dataset_id          INTEGER NOT NULL REFERENCES datasets (dataset_id),
    name                VARCHAR    NOT NULL,
    file_type_id        INTEGER NOT NULL REFERENCES file_types (file_type_id),
    size_in_bytes       DOUBLE,
    md5                 VARCHAR,
    url                 VARCHAR,
    is_from_zip_file    BOOLEAN NOT NULL,
    parent_zip_file_id  INTEGER REFERENCES files (file_id)
);

CREATE SEQUENCE IF NOT EXISTS annotations_seq START 1;
CREATE TABLE IF NOT EXISTS annotations (
    annotation_id       INTEGER PRIMARY KEY DEFAULT nextval('annotations_seq'),
    dataset_id          INTEGER NOT NULL REFERENCES datasets (dataset_id),
    provenance_type_id  INTEGER NOT NULL REFERENCES provenance_types (provenance_id),
    annotation_type_id  INTEGER NOT NULL REFERENCES annotation_types (annotation_type_id),
    file_id             INTEGER REFERENCES files (file_id),
    paper_id            INTEGER REFERENCES papers (paper_id),
    value               VARCHAR    NOT NULL,
    quality_score       VARCHAR,
    value_extra         VARCHAR,
    comment             VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS molecules_seq START 1;
CREATE TABLE IF NOT EXISTS molecules (
    molecule_id       INTEGER PRIMARY KEY DEFAULT nextval('molecules_seq'),
    annotation_id     INTEGER NOT NULL REFERENCES annotations (annotation_id),
    name              VARCHAR    NOT NULL,
    formula           VARCHAR    NOT NULL,
    sequence          VARCHAR    NOT NULL,
    molecule_type_id  INTEGER REFERENCES molecule_types (molecule_type_id)
);

-- ── Simulation File Tables ────────────────────────────────────

CREATE TABLE IF NOT EXISTS topology_files (
    file_id        INTEGER PRIMARY KEY REFERENCES files (file_id),
    atom_number    INTEGER NOT NULL,
    has_protein    BOOLEAN NOT NULL,
    has_nucleic    BOOLEAN NOT NULL,
    has_lipid      BOOLEAN NOT NULL,
    has_glucid     BOOLEAN NOT NULL,
    has_water_ion  BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS parameter_files (
    file_id      INTEGER PRIMARY KEY REFERENCES files (file_id),
    dt           DOUBLE,
    nsteps       INTEGER,
    temperature  DOUBLE,
    thermostat   VARCHAR,
    barostat     VARCHAR,
    integrator   VARCHAR
);

CREATE TABLE IF NOT EXISTS trajectory_files (
    file_id       INTEGER PRIMARY KEY REFERENCES files (file_id),
    atom_number   INTEGER NOT NULL,
    frame_number  INTEGER NOT NULL
);

CREATE SEQUENCE IF NOT EXISTS molecules_external_db_seq START 1;
CREATE TABLE IF NOT EXISTS molecules_external_db (
    mol_ext_db_id       INTEGER PRIMARY KEY DEFAULT nextval('molecules_external_db_seq'),
    molecule_id         INTEGER NOT NULL REFERENCES molecules (molecule_id),
    db_name             VARCHAR    NOT NULL,
    id_in_external_db   VARCHAR    NOT NULL,
    database_id         INTEGER REFERENCES databases (database_id)
);

-- ── Indexes ───────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_files_is_from_zip_file
    ON files (is_from_zip_file);

CREATE INDEX IF NOT EXISTS idx_mol_ext_db_db_name
    ON molecules_external_db (db_name);

-- ── Many-to-Many Link Tables ──────────────────────────────────

CREATE TABLE IF NOT EXISTS datasets_authors_link (
    dataset_id  INTEGER NOT NULL REFERENCES datasets (dataset_id),
    author_id   INTEGER NOT NULL REFERENCES authors (author_id),
    PRIMARY KEY (dataset_id, author_id)
);

CREATE TABLE IF NOT EXISTS authors_papers_link (
    author_id  INTEGER NOT NULL REFERENCES authors (author_id),
    paper_id   INTEGER NOT NULL REFERENCES papers (paper_id),
    PRIMARY KEY (author_id, paper_id)
);