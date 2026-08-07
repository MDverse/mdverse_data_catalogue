-- Download files of a specific file type
SELECT d.id_in_data_source AS dataset_id,
    d.data_source_label AS dataset_origin,
    d.date_created AS date_created,
    f.name AS file_name,
    f.size_in_bytes AS file_size_in_bytes,
    f.is_from_zip_file AS is_file_from_zip_file,
    CASE
        WHEN f.is_from_zip_file = 1 THEN pf.url
        ELSE f.url
    END AS file_url,
    d.url_in_data_source AS dataset_url
FROM files f
    JOIN datasets d ON f.dataset_id = d.dataset_id
    LEFT JOIN files pf ON f.parent_zip_file_id = pf.file_id
WHERE LOWER(f.file_type_label) = LOWER(?);