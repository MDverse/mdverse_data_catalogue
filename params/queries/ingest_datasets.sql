-- Insert unique data sources.
INSERT INTO data_sources (data_source_label, url, citation, comment)
SELECT data_source_label,
    FIRST(data_source_url),
    FIRST(data_source_citation),
    FIRST(data_source_comment)
FROM _stage_datasets
WHERE data_source_label IS NOT NULL
    AND TRIM(data_source_label) != ''
GROUP BY data_source_label ON CONFLICT (data_source_label) DO
UPDATE
SET url = COALESCE(data_sources.url, EXCLUDED.url),
    citation = COALESCE(data_sources.citation, EXCLUDED.citation),
    comment = COALESCE(data_sources.comment, EXCLUDED.comment);
-- Insert unique databases.
INSERT INTO databases (database_label, url, comment)
SELECT DISTINCT database_label,
    url,
    comment
FROM _stage_ref_databases
WHERE database_label IS NOT NULL
    AND TRIM(database_label) != '' ON CONFLICT (database_label) DO
UPDATE
SET url = EXCLUDED.url,
    comment = COALESCE(databases.comment, EXCLUDED.comment);
-- Insert unique projects.
INSERT INTO projects (project_label, url, citation, comment)
SELECT project_label,
    FIRST(project_url),
    FIRST(project_citation),
    FIRST(project_comment)
FROM _stage_datasets
WHERE project_label IS NOT NULL
    AND project_label NOT IN (
        SELECT project_label
        FROM projects
        WHERE project_label IS NOT NULL
    )
GROUP BY project_label;
-- Insert unique persons.
INSERT INTO persons (
        full_name,
        orcid,
        first_name,
        last_name,
        affiliation
    )
SELECT full_name,
    FIRST(orcid),
    FIRST(first_name),
    FIRST(last_name),
    FIRST(affiliation)
FROM _stage_authors
WHERE full_name IS NOT NULL
    AND TRIM(full_name) != ''
GROUP BY full_name ON CONFLICT (full_name) DO
UPDATE
SET orcid = COALESCE(persons.orcid, EXCLUDED.orcid),
    first_name = COALESCE(persons.first_name, EXCLUDED.first_name),
    last_name = COALESCE(persons.last_name, EXCLUDED.last_name),
    affiliation = COALESCE(persons.affiliation, EXCLUDED.affiliation);
-- Insert publications linked to datasets
INSERT INTO publications (
        data_source_label,
        id_in_data_source,
        doi,
        title,
        year,
        url,
        abstract,
        journal,
        keywords
    )
SELECT data_source_label,
    id_in_data_source,
    doi,
    title,
    year,
    url,
    abstract,
    journal,
    keywords
FROM _stage_publications
WHERE doi IS NOT NULL
    AND doi != '' ON CONFLICT (doi) DO
UPDATE
SET title = COALESCE(EXCLUDED.title, publications.title),
    year = COALESCE(EXCLUDED.year, publications.year),
    url = COALESCE(EXCLUDED.url, publications.url),
    abstract = COALESCE(EXCLUDED.abstract, publications.abstract),
    journal = COALESCE(EXCLUDED.journal, publications.journal),
    keywords = COALESCE(EXCLUDED.keywords, publications.keywords);
-- Insert datasets into the main datasets table.
INSERT INTO datasets (
        data_source_label,
        id_in_data_source,
        url_in_data_source,
        project_label,
        id_in_project,
        url_in_project,
        doi,
        date_created,
        date_last_updated,
        date_last_fetched,
        title,
        description,
        keywords,
        file_number,
        download_number,
        view_number
    )
SELECT s.data_source_label,
    s.id_in_data_source,
    s.url_in_data_source,
    s.project_label,
    s.id_in_project,
    s.url_in_project,
    s.doi,
    s.date_created,
    s.date_last_updated,
    s.date_last_fetched,
    s.title,
    s.description,
    s.keywords,
    s.file_number,
    s.download_number,
    s.view_number
FROM _stage_datasets s ON CONFLICT (data_source_label, id_in_data_source) DO
UPDATE
SET title = EXCLUDED.title,
    description = EXCLUDED.description,
    url_in_data_source = EXCLUDED.url_in_data_source,
    date_created = EXCLUDED.date_created,
    date_last_updated = EXCLUDED.date_last_updated,
    date_last_fetched = EXCLUDED.date_last_fetched,
    keywords = EXCLUDED.keywords,
    file_number = EXCLUDED.file_number,
    download_number = EXCLUDED.download_number,
    view_number = EXCLUDED.view_number;
-- Insert dataset-author relationships.
INSERT INTO datasets_authors_link (dataset_id, person_id)
SELECT DISTINCT d.dataset_id,
    p.person_id
FROM _stage_authors sa
    JOIN datasets d ON d.id_in_data_source = sa.id_in_data_source
    AND d.data_source_label = sa.data_source_label
    JOIN persons p ON p.full_name = sa.full_name ON CONFLICT DO NOTHING;
-- Insert dataset-publication relationships using _stage_publication_links.
INSERT INTO datasets_publications_link (dataset_id, publication_id)
SELECT DISTINCT d.dataset_id,
    p.publication_id
FROM _stage_publication_links spl
    JOIN datasets d ON d.id_in_data_source = spl.id_in_data_source
    AND d.data_source_label = spl.data_source_label
    JOIN publications p ON p.doi = spl.doi ON CONFLICT DO NOTHING;
-- Insert dataset annotations.
INSERT INTO annotations (
        dataset_id,
        value,
        category_label,
        provenance_label,
        quality_score
    )
SELECT DISTINCT d.dataset_id,
    sa.value,
    sa.category_label,
    sa.provenance_label,
    CASE
        WHEN sa.provenance_label IN ('Provided_by_database', 'Manually_annotated') THEN 1.0
        ELSE sa.quality_score
    END AS quality_score
FROM _stage_annotations sa
    JOIN datasets d ON d.data_source_label = sa.data_source_label
    AND d.id_in_data_source = sa.id_in_data_source ON CONFLICT (
        value,
        category_label,
        dataset_id,
        publication_id,
        file_id
    ) DO
UPDATE
SET quality_score = EXCLUDED.quality_score;
-- Insert molecules and link them to annotations.
INSERT INTO molecules (
        name,
        formula,
        sequence,
        organism,
        molecule_type_label,
        annotation_id
    )
SELECT sm.name,
    sm.formula,
    sm.sequence,
    sm.organism,
    sm.molecule_type_label,
    a.annotation_id
FROM _stage_molecules sm
    JOIN _stage_annotations sa ON sm.temp_mol_id = sa.temp_mol_id
    JOIN datasets d ON d.data_source_label = sa.data_source_label
    AND d.id_in_data_source = sa.id_in_data_source
    JOIN annotations a ON a.dataset_id = d.dataset_id
    AND a.value = sa.value
    AND a.category_label = sa.category_label;
-- Insert external database links for molecules.
INSERT INTO molecules_external_databases (
        molecule_id,
        database_label,
        id_in_external_database,
        url_in_external_database
    )
SELECT DISTINCT CAST(m.molecule_id AS INTEGER) AS molecule_id,
    CAST(se.database_label AS VARCHAR) AS database_label,
    se.id_in_external_database,
    se.url_in_external_database
FROM _stage_molecules_ext_db se
    JOIN _stage_annotations sa ON se.temp_mol_id = sa.temp_mol_id
    JOIN datasets d ON d.data_source_label = sa.data_source_label
    AND d.id_in_data_source = sa.id_in_data_source
    JOIN annotations a ON a.dataset_id = d.dataset_id
    AND a.value = sa.value
    AND a.category_label = 'MOL'
    JOIN molecules m ON m.annotation_id = a.annotation_id ON CONFLICT (molecule_id, database_label) DO NOTHING;
-- Insert dataset-author relationships.
INSERT INTO datasets_authors_link (dataset_id, person_id)
SELECT DISTINCT d.dataset_id,
    p.person_id
FROM _stage_authors sa
    JOIN datasets d ON d.data_source_label = sa.data_source_label
    AND d.id_in_data_source = sa.id_in_data_source
    JOIN persons p ON p.full_name = sa.full_name ON CONFLICT DO NOTHING;