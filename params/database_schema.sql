-- MDverse database schema

-- Lookup / Type tables
-- No foreign keys — created first, safe to reference immediately.

CREATE SEQUENCE IF NOT EXISTS file_type_id_sequence START 1;
CREATE TABLE IF NOT EXISTS file_types (
    file_type_id  INTEGER PRIMARY KEY DEFAULT nextval('file_type_id_sequence'),
    name          VARCHAR    NOT NULL UNIQUE,
    comment       VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS molecule_type_id_sequence START 1;
CREATE TABLE IF NOT EXISTS molecule_types (
    molecule_type_id  INTEGER PRIMARY KEY DEFAULT nextval('molecule_type_id_sequence'),
    name              VARCHAR    NOT NULL UNIQUE,
    comment           VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS database_id_sequence START 1;
CREATE TABLE IF NOT EXISTS databases (
    database_id  INTEGER PRIMARY KEY DEFAULT nextval('database_id_sequence'),
    name         VARCHAR    NOT NULL UNIQUE,
    url          VARCHAR,
    comment      VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS data_source_id_sequence START 1;
CREATE TABLE IF NOT EXISTS data_sources (
    data_source_id  INTEGER PRIMARY KEY DEFAULT nextval('data_source_id_sequence'),
    name            VARCHAR    NOT NULL UNIQUE,
    url             VARCHAR,
    citation        VARCHAR,
    comment         VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS provenance_type_id_sequence START 1;
CREATE TABLE IF NOT EXISTS provenance_types (
    provenance_id  INTEGER PRIMARY KEY DEFAULT nextval('provenance_type_id_sequence'),
    name           VARCHAR    NOT NULL UNIQUE,
    comment        VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS annotation_type_id_sequence START 1;
CREATE TABLE IF NOT EXISTS annotation_types (
    annotation_type_id  INTEGER PRIMARY KEY DEFAULT nextval('annotation_type_id_sequence'),
    label               VARCHAR    NOT NULL,
    name                VARCHAR    NOT NULL,
    comment             VARCHAR,
    UNIQUE (name, label)
);

---------------------------------------------------------------------
-- Main tables
---------------------------------------------------------------------

CREATE SEQUENCE IF NOT EXISTS author_id_sequence START 1;
CREATE TABLE IF NOT EXISTS authors (
    author_id  INTEGER PRIMARY KEY DEFAULT nextval('author_id_sequence'),
    name       VARCHAR    NOT NULL,
    orcid      VARCHAR,
    UNIQUE (name, orcid)
);

CREATE SEQUENCE IF NOT EXISTS paper_id_sequence START 1;
CREATE TABLE IF NOT EXISTS papers (
    paper_id  INTEGER PRIMARY KEY DEFAULT nextval('paper_id_sequence'),
    doi       VARCHAR,
    title     VARCHAR    NOT NULL,
    abstract  VARCHAR,
    journal   VARCHAR    NOT NULL,
    url       VARCHAR,
    year      VARCHAR,
    keywords  VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS project_id_sequence START 1;
CREATE TABLE IF NOT EXISTS projects (
    project_id  INTEGER PRIMARY KEY DEFAULT nextval('project_id_sequence'),
    name        VARCHAR    NOT NULL UNIQUE,
    url         VARCHAR    NOT NULL,
    comment     VARCHAR,
    citation    VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS dataset_id_sequence START 1;
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id          INTEGER PRIMARY KEY DEFAULT nextval('dataset_id_sequence'),
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

CREATE SEQUENCE IF NOT EXISTS file_id_sequence START 1;
CREATE TABLE IF NOT EXISTS files (
    file_id             INTEGER PRIMARY KEY DEFAULT nextval('file_id_sequence'),
    dataset_id          INTEGER NOT NULL REFERENCES datasets (dataset_id),
    name                VARCHAR    NOT NULL,
    file_type_id        INTEGER NOT NULL REFERENCES file_types (file_type_id),
    size_in_bytes       DOUBLE,
    md5                 VARCHAR,
    url                 VARCHAR,
    is_from_zip_file    BOOLEAN NOT NULL,
    parent_zip_file_id  INTEGER REFERENCES files (file_id)
);

CREATE SEQUENCE IF NOT EXISTS annotation_id_sequence START 1;
CREATE TABLE IF NOT EXISTS annotations (
    annotation_id       INTEGER PRIMARY KEY DEFAULT nextval('annotation_id_sequence'),
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

CREATE SEQUENCE IF NOT EXISTS molecule_id_sequence START 1;
CREATE TABLE IF NOT EXISTS molecules (
    molecule_id       INTEGER PRIMARY KEY DEFAULT nextval('molecule_id_sequence'),
    annotation_id     INTEGER NOT NULL REFERENCES annotations (annotation_id),
    name              VARCHAR    NOT NULL,
    formula           VARCHAR    NOT NULL,
    sequence          VARCHAR    NOT NULL,
    molecule_type_id  INTEGER REFERENCES molecule_types (molecule_type_id)
);

-- Simulation files tables

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

CREATE SEQUENCE IF NOT EXISTS molecules_external_db_id_sequence START 1;
CREATE TABLE IF NOT EXISTS molecules_external_db (
    mol_ext_db_id       INTEGER PRIMARY KEY DEFAULT nextval('molecules_external_db_id_sequence'),
    molecule_id         INTEGER NOT NULL REFERENCES molecules (molecule_id),
    db_name             VARCHAR    NOT NULL,
    id_in_external_db   VARCHAR    NOT NULL,
    database_id         INTEGER REFERENCES databases (database_id)
);

-- Indexes for faster queries

CREATE INDEX IF NOT EXISTS idx_files_is_from_zip_file
    ON files (is_from_zip_file);

CREATE INDEX IF NOT EXISTS idx_mol_ext_db_db_name
    ON molecules_external_db (db_name);


-- Many-to-Many tables

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
