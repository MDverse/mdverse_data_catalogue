-- MDverse database schema

-- Lookup / Type tables
-- No foreign keys — created first, safe to reference immediately.

CREATE TABLE IF NOT EXISTS file_types (
    file_type_label  VARCHAR PRIMARY KEY,  -- e.g. "MDP", "GRO", "TXT"...
    comment          VARCHAR UNIQUE
);

CREATE TABLE IF NOT EXISTS molecule_types (
    molecule_type_label  VARCHAR PRIMARY KEY,  -- e.g. "PROTEIN", "LIPID", "NUCLEIC ACID"...
    description           VARCHAR
);

CREATE TABLE IF NOT EXISTS databases (
    database_label  VARCHAR PRIMARY KEY,  -- e.g. "UniProt", "PDB", "ChEBI"...
    url             VARCHAR NOT NULL,
    comment         VARCHAR
);

CREATE TABLE IF NOT EXISTS data_sources (
    data_source_label  VARCHAR PRIMARY KEY,  -- e.g. "Zenodo", "Figshare", "NOMAD"...
    url                VARCHAR NOT NULL,
    citation           VARCHAR,
    comment            VARCHAR
);

CREATE TABLE IF NOT EXISTS projects (
    project_label  VARCHAR PRIMARY KEY,  -- e.g. "MDDB", "NMRlipids"...
    url            VARCHAR,
    comment        VARCHAR,
    citation       VARCHAR
);

CREATE TABLE IF NOT EXISTS annotation_provenances (
    provenance_label  VARCHAR PRIMARY KEY,           -- e.g. "Manually annotated",
                                                          -- "Extracted automatically",
                                                          -- "Provided by database"
    comment           VARCHAR UNIQUE
);

INSERT OR IGNORE INTO annotation_provenances (provenance_label, comment) VALUES
    ('Manually annotated', 'This annotation was manually curated by a human annotator.'),
    ('Provided by database', 'This annotation was provided by the original database or data source.'),
    ('Extracted automatically', 'This annotation was extracted automatically by Artificial Intelligence.');

CREATE TABLE IF NOT EXISTS annotation_categories (
    category_label  VARCHAR PRIMARY KEY,             -- e.g. "MOL"
    label           VARCHAR NOT NULL UNIQUE,              -- "Molecule_names"
    description     VARCHAR
);

INSERT OR IGNORE INTO annotation_categories (category_label, label) VALUES
    ('MOL', 'Molecule names'),
    ('SOFTWARE', 'Software'),
    ('FORCEFIELD_MODEL', 'Forcefield or model'),
    ('SIMULATION_TIMESTEP', 'Simulation timestep'),
    ('SIMULATION_TIME', 'Simulation time'),
    ('SIMULATION_TEMPERATURE', 'Simulation temperature');

---------------------------------------------------------------------
-- Main tables
---------------------------------------------------------------------

CREATE SEQUENCE IF NOT EXISTS person_id_sequence START 1;
CREATE TABLE IF NOT EXISTS persons (
    person_id    INTEGER PRIMARY KEY DEFAULT nextval('person_id_sequence'),
    full_name    VARCHAR UNIQUE,
    orcid        VARCHAR UNIQUE,
    first_name   VARCHAR,
    last_name    VARCHAR,
    affiliation  VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS paper_id_sequence START 1;
CREATE TABLE IF NOT EXISTS papers (
    paper_id  INTEGER PRIMARY KEY DEFAULT nextval('paper_id_sequence'),
    doi       VARCHAR NOT NULL UNIQUE,
    title     VARCHAR,
    year      VARCHAR,
    url       VARCHAR,
    abstract  VARCHAR,
    journal   VARCHAR,
    keywords  VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS dataset_id_sequence START 1;
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id          INTEGER PRIMARY KEY DEFAULT nextval('dataset_id_sequence'),
    data_source_label   VARCHAR NOT NULL REFERENCES data_sources (data_source_label),
    id_in_data_source   VARCHAR NOT NULL,
    url_in_data_source  VARCHAR,
    project_label       VARCHAR REFERENCES projects (project_label),
    id_in_project       VARCHAR,
    url_in_project      VARCHAR,
    doi                 VARCHAR UNIQUE,
    date_created        VARCHAR,                     -- format YYYY-MM-DD
    date_last_updated  VARCHAR,                     -- format YYYY-MM-DD
    date_last_fetched   VARCHAR NOT NULL,            -- format YYYY-MM-DDTHH:MM:SS
    file_number         INTEGER,
    download_number     INTEGER,
    view_number         INTEGER,
    license             VARCHAR,
    title               VARCHAR,
    description         VARCHAR,
    keywords            VARCHAR,
    UNIQUE (data_source_label, id_in_data_source)
);

CREATE SEQUENCE IF NOT EXISTS file_id_sequence START 1;
CREATE TABLE IF NOT EXISTS files (
    file_id             INTEGER PRIMARY KEY DEFAULT nextval('file_id_sequence'),
    dataset_id          INTEGER NOT NULL REFERENCES datasets (dataset_id),
    name                VARCHAR NOT NULL,
    file_type_label     VARCHAR NOT NULL REFERENCES file_types (file_type_label),
    size_in_bytes       DOUBLE,
    size_human_readable  VARCHAR,
    md5                 VARCHAR,
    url                 VARCHAR,
    is_from_zip_file    BOOLEAN NOT NULL,
    parent_zip_file_id  INTEGER REFERENCES files (file_id)
);

CREATE SEQUENCE IF NOT EXISTS annotation_id_sequence START 1;
CREATE TABLE IF NOT EXISTS annotations (
    annotation_id     INTEGER PRIMARY KEY DEFAULT nextval('annotation_id_sequence'),
    dataset_id        INTEGER NOT NULL REFERENCES datasets (dataset_id),
    paper_id          INTEGER REFERENCES papers (paper_id),
    file_id           INTEGER REFERENCES files (file_id),
    value             VARCHAR NOT NULL,
    category_label    VARCHAR NOT NULL REFERENCES annotation_categories (category_label),
    provenance_label  VARCHAR NOT NULL REFERENCES annotation_provenances (provenance_label),
    quality_score     DOUBLE,
    value_extra       VARCHAR,
    comment           VARCHAR,
    UNIQUE (value, category_label, dataset_id, paper_id, file_id)
);

CREATE SEQUENCE IF NOT EXISTS molecule_id_sequence START 1;
CREATE TABLE IF NOT EXISTS molecules (
    molecule_id           INTEGER PRIMARY KEY DEFAULT nextval('molecule_id_sequence'),
    name                  VARCHAR,
    formula               VARCHAR,
    sequence              VARCHAR NOT NULL,
    organism              VARCHAR,
    molecule_type_label   VARCHAR REFERENCES molecule_types (molecule_type_label),
    annotation_id         INTEGER NOT NULL REFERENCES annotations (annotation_id)
);

-- Many-to-Many tables

CREATE TABLE IF NOT EXISTS molecules_external_databases (
    molecule_id               INTEGER NOT NULL REFERENCES molecules (molecule_id),
    database_label            VARCHAR NOT NULL REFERENCES databases (database_label),
    id_in_external_database   VARCHAR UNIQUE,
    url_in_external_database  VARCHAR UNIQUE,
    PRIMARY KEY (molecule_id, database_label)
);

CREATE TABLE IF NOT EXISTS datasets_authors_link (
    dataset_id  INTEGER NOT NULL REFERENCES datasets (dataset_id),
    person_id   INTEGER NOT NULL REFERENCES persons (person_id),
    PRIMARY KEY (dataset_id, person_id)
);

CREATE TABLE IF NOT EXISTS authors_papers_link (
    person_id  INTEGER NOT NULL REFERENCES persons (person_id),
    paper_id   INTEGER NOT NULL REFERENCES papers (paper_id),
    PRIMARY KEY (person_id, paper_id)
);

CREATE TABLE IF NOT EXISTS datasets_papers_link (
    dataset_id INTEGER NOT NULL,
    paper_id   INTEGER NOT NULL,
    PRIMARY KEY (dataset_id, paper_id),
    FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id),
    FOREIGN KEY (paper_id) REFERENCES papers(paper_id)
);
-- Indexes for faster queries

CREATE INDEX IF NOT EXISTS idx_files_is_from_zip_file
    ON files (is_from_zip_file);

CREATE INDEX IF NOT EXISTS idx_molecules_external_databases_database_label
    ON molecules_external_databases (database_label);
