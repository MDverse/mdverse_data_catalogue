-- MDverse database schema
-- Lookup / Type tables
-- No foreign keys — created first, safe to reference immediately.
CREATE TABLE IF NOT EXISTS file_types (
    file_type_label VARCHAR PRIMARY KEY,
    -- e.g. "MDP", "GRO", "TXT"...
    comment VARCHAR UNIQUE
);
CREATE TABLE IF NOT EXISTS molecule_types (
    molecule_type_label VARCHAR PRIMARY KEY,
    -- e.g. "PROTEIN", "LIPID", "NUCLEIC ACID"...
    description VARCHAR
);
CREATE TABLE IF NOT EXISTS databases (
    database_label VARCHAR PRIMARY KEY,
    -- e.g. "UniProt", "PDB", "ChEBI"...
    url VARCHAR NOT NULL,
    comment VARCHAR
);
CREATE TABLE IF NOT EXISTS data_sources (
    data_source_label VARCHAR PRIMARY KEY,
    -- e.g. "Zenodo", "Figshare", "NOMAD"...
    url VARCHAR NOT NULL,
    citation VARCHAR,
    comment VARCHAR
);
CREATE TABLE IF NOT EXISTS projects (
    project_label VARCHAR PRIMARY KEY,
    -- e.g. "MDDB", "NMRlipids"...
    url VARCHAR,
    comment VARCHAR,
    citation VARCHAR
);
CREATE TABLE IF NOT EXISTS annotation_provenances (
    provenance_label VARCHAR PRIMARY KEY,
    -- e.g. "Manually annotated",
    -- "Extracted automatically",
    -- "Provided by database"
    comment VARCHAR UNIQUE
);
CREATE TABLE IF NOT EXISTS annotation_categories (
    category_label VARCHAR PRIMARY KEY,
    -- e.g. "MOL"
    label VARCHAR NOT NULL UNIQUE,
    -- "Molecule_names"
    description VARCHAR
);
---------------------------------------------------------------------
-- Main tables
---------------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS person_id_sequence START 1;
CREATE TABLE IF NOT EXISTS persons (
    person_id INTEGER PRIMARY KEY DEFAULT nextval('person_id_sequence'),
    full_name VARCHAR NOT NULL UNIQUE,
    orcid VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    affiliation VARCHAR
);
CREATE SEQUENCE IF NOT EXISTS publication_id_sequence START 1;
CREATE TABLE IF NOT EXISTS publications (
    publication_id INTEGER PRIMARY KEY DEFAULT nextval('publication_id_sequence'),
    data_source_label VARCHAR NOT NULL REFERENCES data_sources (data_source_label),
    id_in_data_source VARCHAR NOT NULL,
    url VARCHAR,
    doi VARCHAR NOT NULL UNIQUE,
    title VARCHAR,
    year VARCHAR,
    abstract VARCHAR,
    journal VARCHAR,
    keywords VARCHAR,
    external_links VARCHAR [],
    UNIQUE (data_source_label, id_in_data_source)
);
CREATE SEQUENCE IF NOT EXISTS dataset_id_sequence START 1;
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id INTEGER PRIMARY KEY DEFAULT nextval('dataset_id_sequence'),
    data_source_label VARCHAR NOT NULL REFERENCES data_sources (data_source_label),
    id_in_data_source VARCHAR NOT NULL,
    url_in_data_source VARCHAR,
    project_label VARCHAR REFERENCES projects (project_label),
    id_in_project VARCHAR,
    url_in_project VARCHAR,
    doi VARCHAR,
    date_created VARCHAR,
    -- format YYYY-MM-DD
    date_last_updated VARCHAR,
    -- format YYYY-MM-DD
    date_last_fetched VARCHAR NOT NULL,
    -- format YYYY-MM-DDTHH:MM:SS
    file_number INTEGER,
    download_number INTEGER,
    view_number INTEGER,
    license VARCHAR,
    title VARCHAR,
    description VARCHAR,
    keywords VARCHAR,
    UNIQUE (data_source_label, id_in_data_source)
);
CREATE SEQUENCE IF NOT EXISTS file_id_sequence START 1;
CREATE TABLE IF NOT EXISTS files (
    file_id INTEGER PRIMARY KEY DEFAULT nextval('file_id_sequence'),
    dataset_id INTEGER NOT NULL REFERENCES datasets (dataset_id),
    name VARCHAR NOT NULL,
    file_type_label VARCHAR NOT NULL REFERENCES file_types (file_type_label),
    size_in_bytes DOUBLE,
    size_human_readable VARCHAR,
    md5 VARCHAR,
    url VARCHAR,
    is_from_zip_file BOOLEAN NOT NULL,
    parent_zip_file_id INTEGER REFERENCES files (file_id)
);
CREATE SEQUENCE IF NOT EXISTS ai_model_id_sequence START 1;
CREATE TABLE IF NOT EXISTS ai_models (
    ai_model_id INTEGER PRIMARY KEY DEFAULT nextval('ai_model_id_sequence'),
    data_source_label VARCHAR NOT NULL REFERENCES data_sources (data_source_label),
    id_in_data_source VARCHAR NOT NULL,
    url_in_data_source VARCHAR NOT NULL,
    doi VARCHAR,
    description VARCHAR,
    license VARCHAR,
    tasks VARCHAR,
    number_of_parameters BIGINT,
    keywords VARCHAR,
    -- Stored as comma-separated string
    date_created VARCHAR,
    -- format YYYY-MM-DD
    date_last_updated VARCHAR,
    -- format YYYY-MM-DD
    date_last_fetched VARCHAR NOT NULL,
    -- format YYYY-MM-DDTHH:MM:SS
    UNIQUE (data_source_label, id_in_data_source)
);
CREATE SEQUENCE IF NOT EXISTS annotation_id_sequence START 1;
CREATE TABLE IF NOT EXISTS annotations (
    annotation_id INTEGER PRIMARY KEY DEFAULT nextval('annotation_id_sequence'),
    dataset_id INTEGER REFERENCES datasets (dataset_id),
    publication_id INTEGER REFERENCES publications (publication_id),
    file_id INTEGER REFERENCES files (file_id),
    annotation_value VARCHAR NOT NULL,
    category_label VARCHAR NOT NULL REFERENCES annotation_categories (category_label),
    provenance_label VARCHAR NOT NULL REFERENCES annotation_provenances (provenance_label),
    quality_score DOUBLE CHECK (
        quality_score IS NULL
        OR (
            quality_score >= 0.0
            AND quality_score <= 1.0
        )
    ),
    comment VARCHAR,
    -- Avoid duplicate annotations for the same dataset/publication/file and category.
    UNIQUE (
        annotation_value,
        category_label,
        dataset_id,
        publication_id,
        file_id
    )
);
CREATE SEQUENCE IF NOT EXISTS molecule_id_sequence START 1;
CREATE TABLE IF NOT EXISTS molecules (
    molecule_id INTEGER PRIMARY KEY DEFAULT nextval('molecule_id_sequence'),
    name VARCHAR,
    formula VARCHAR,
    sequence VARCHAR,
    organism VARCHAR,
    molecule_type_label VARCHAR REFERENCES molecule_types (molecule_type_label),
    annotation_id INTEGER NOT NULL REFERENCES annotations (annotation_id)
);
-- Many-to-Many tables
CREATE TABLE IF NOT EXISTS molecules_external_databases (
    molecule_id INTEGER NOT NULL REFERENCES molecules (molecule_id),
    database_label VARCHAR NOT NULL REFERENCES databases (database_label),
    id_in_external_database VARCHAR,
    url_in_external_database VARCHAR,
    PRIMARY KEY (molecule_id, database_label)
);
CREATE TABLE IF NOT EXISTS datasets_authors_link (
    dataset_id INTEGER NOT NULL REFERENCES datasets (dataset_id),
    person_id INTEGER NOT NULL REFERENCES persons (person_id),
    PRIMARY KEY (dataset_id, person_id)
);
CREATE TABLE IF NOT EXISTS authors_publications_link (
    person_id INTEGER NOT NULL REFERENCES persons (person_id),
    publication_id INTEGER NOT NULL REFERENCES publications (publication_id),
    PRIMARY KEY (person_id, publication_id)
);
CREATE TABLE IF NOT EXISTS datasets_publications_link (
    dataset_id INTEGER NOT NULL,
    publication_id INTEGER NOT NULL,
    PRIMARY KEY (dataset_id, publication_id),
    FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id),
    FOREIGN KEY (publication_id) REFERENCES publications(publication_id)
);
CREATE TABLE IF NOT EXISTS models_authors_link (
    ai_model_id INTEGER NOT NULL REFERENCES ai_models (ai_model_id),
    person_id INTEGER NOT NULL REFERENCES persons (person_id),
    PRIMARY KEY (ai_model_id, person_id)
);
CREATE TABLE IF NOT EXISTS publications_models_link (
    publication_id INTEGER NOT NULL REFERENCES publications (publication_id),
    ai_model_id INTEGER NOT NULL REFERENCES ai_models (ai_model_id),
    PRIMARY KEY (publication_id, ai_model_id)
);
CREATE TABLE IF NOT EXISTS datasets_models_link (
    dataset_id INTEGER NOT NULL REFERENCES datasets (dataset_id),
    ai_model_id INTEGER NOT NULL REFERENCES ai_models (ai_model_id),
    PRIMARY KEY (dataset_id, ai_model_id)
);
-- Indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_files_is_from_zip_file ON files (is_from_zip_file);
CREATE INDEX IF NOT EXISTS idx_molecules_external_databases_database_label ON molecules_external_databases (database_label);
CREATE INDEX IF NOT EXISTS idx_ai_models_id_in_data_source ON ai_models (data_source_label, id_in_data_source);