BEGIN TRANSACTION;
-- Insert data sources from publications.
INSERT INTO data_sources (data_source_label, url, citation, comment)
SELECT data_source_label,
    FIRST(data_source_url),
    FIRST(data_source_citation),
    FIRST(data_source_comment)
FROM _stage_publications
WHERE data_source_label IS NOT NULL
    AND TRIM(data_source_label) != ''
GROUP BY data_source_label ON CONFLICT (data_source_label) DO
UPDATE
SET url = COALESCE(data_sources.url, EXCLUDED.url),
    citation = COALESCE(data_sources.citation, EXCLUDED.citation),
    comment = COALESCE(data_sources.comment, EXCLUDED.comment);
-- Insert publications metadata including external links.
INSERT INTO publications (
        data_source_label,
        id_in_data_source,
        doi,
        title,
        year,
        url,
        abstract,
        journal,
        keywords,
        external_links
    )
SELECT data_source_label,
    id_in_data_source,
    doi,
    title,
    year,
    url,
    abstract,
    journal,
    keywords,
    external_links
FROM _stage_publications ON CONFLICT (data_source_label, id_in_data_source) DO
UPDATE
SET doi = COALESCE(EXCLUDED.doi, publications.doi),
    title = EXCLUDED.title,
    year = EXCLUDED.year,
    url = EXCLUDED.url,
    abstract = EXCLUDED.abstract,
    journal = EXCLUDED.journal,
    keywords = EXCLUDED.keywords,
    external_links = EXCLUDED.external_links;
-- Insert publication authors.
INSERT INTO persons (
        full_name,
        orcid,
        first_name,
        last_name,
        affiliation
    )
SELECT DISTINCT full_name,
    orcid,
    first_name,
    last_name,
    affiliation
FROM _stage_publications_authors
WHERE full_name IS NOT NULL
    AND TRIM(full_name) != '' ON CONFLICT (full_name) DO
UPDATE
SET orcid = COALESCE(EXCLUDED.orcid, persons.orcid),
    affiliation = COALESCE(EXCLUDED.affiliation, persons.affiliation);
-- Link publications and authors.
INSERT INTO authors_publications_link (person_id, publication_id)
SELECT DISTINCT p.person_id,
    pub.publication_id
FROM _stage_publications_authors spa
    JOIN persons p ON p.full_name = spa.full_name
    JOIN publications pub ON (
        spa.doi IS NOT NULL
        AND TRIM(spa.doi) != ''
        AND pub.doi = spa.doi
    )
    OR (
        pub.data_source_label = spa.data_source_label
        AND pub.id_in_data_source = spa.id_in_data_source
    ) ON CONFLICT DO NOTHING;
-- Insert resolved AI models.
INSERT INTO ai_models (
        data_source_label,
        id_in_data_source,
        url_in_data_source,
        doi,
        description,
        license,
        tasks,
        number_of_parameters,
        keywords,
        date_created,
        date_last_updated,
        date_last_fetched
    )
SELECT data_source_label,
    id_in_data_source,
    url_in_data_source,
    doi,
    description,
    license,
    tasks,
    number_of_parameters,
    keywords,
    date_created,
    date_last_updated,
    date_last_fetched
FROM _stage_ai_models ON CONFLICT (data_source_label, id_in_data_source) DO
UPDATE
SET description = EXCLUDED.description,
    license = EXCLUDED.license,
    tasks = EXCLUDED.tasks,
    number_of_parameters = EXCLUDED.number_of_parameters,
    keywords = EXCLUDED.keywords,
    date_last_updated = EXCLUDED.date_last_updated,
    date_last_fetched = EXCLUDED.date_last_fetched;
-- Link publications and AI models.
INSERT INTO publications_models_link (publication_id, ai_model_id)
SELECT DISTINCT pub.publication_id,
    m.ai_model_id
FROM _stage_publications_models_link spm
    JOIN publications pub ON (
        spm.doi IS NOT NULL
        AND TRIM(spm.doi) != ''
        AND pub.doi = spm.doi
    )
    OR (
        pub.data_source_label = spm.pub_data_source_label
        AND pub.id_in_data_source = spm.pub_id_in_data_source
    )
    JOIN ai_models m ON m.data_source_label = spm.model_data_source_label
    AND m.id_in_data_source = spm.model_id_in_data_source ON CONFLICT DO NOTHING;
-- Link AI models and authors.
INSERT INTO models_authors_link (ai_model_id, person_id)
SELECT DISTINCT pml.ai_model_id,
    apl.person_id
FROM publications_models_link pml
    JOIN authors_publications_link apl ON pml.publication_id = apl.publication_id ON CONFLICT DO NOTHING;
-- Insert missing datasets discovered via publications.
INSERT INTO datasets (
        data_source_label,
        id_in_data_source,
        url_in_data_source,
        title,
        description,
        doi,
        date_created,
        date_last_updated,
        file_number,
        license,
        download_number,
        view_number,
        keywords,
        date_last_fetched
    )
SELECT data_source_label,
    id_in_data_source,
    url_in_data_source,
    title,
    description,
    doi,
    date_created,
    date_last_updated,
    file_number,
    license,
    download_number,
    view_number,
    keywords,
    date_last_fetched
FROM _stage_resolved_datasets ON CONFLICT (data_source_label, id_in_data_source) DO
UPDATE
SET title = COALESCE(EXCLUDED.title, datasets.title),
    description = COALESCE(EXCLUDED.description, datasets.description),
    doi = COALESCE(EXCLUDED.doi, datasets.doi),
    date_created = COALESCE(EXCLUDED.date_created, datasets.date_created),
    date_last_updated = EXCLUDED.date_last_updated,
    file_number = COALESCE(EXCLUDED.file_number, datasets.file_number),
    license = COALESCE(EXCLUDED.license, datasets.license),
    download_number = COALESCE(
        EXCLUDED.download_number,
        datasets.download_number
    ),
    view_number = COALESCE(EXCLUDED.view_number, datasets.view_number),
    keywords = COALESCE(EXCLUDED.keywords, datasets.keywords),
    date_last_fetched = EXCLUDED.date_last_fetched;
-- Link publications and datasets.
INSERT INTO datasets_publications_link (dataset_id, publication_id)
SELECT DISTINCT d.dataset_id,
    pub.publication_id
FROM _stage_publications_datasets_link spd
    JOIN publications pub ON (
        spd.doi IS NOT NULL
        AND TRIM(spd.doi) != ''
        AND pub.doi = spd.doi
    )
    OR (
        pub.data_source_label = spd.pub_data_source_label
        AND pub.id_in_data_source = spd.pub_id_in_data_source
    )
    JOIN datasets d ON d.data_source_label = spd.dataset_data_source_label
    AND d.id_in_data_source = spd.dataset_id_in_data_source ON CONFLICT DO NOTHING;
-- Link datasets and AI models perfectly.
INSERT INTO datasets_models_link (dataset_id, ai_model_id)
SELECT DISTINCT dpl.dataset_id,
    pml.ai_model_id
FROM datasets_publications_link dpl
    JOIN publications_models_link pml ON dpl.publication_id = pml.publication_id ON CONFLICT DO NOTHING;
COMMIT;