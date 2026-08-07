-- Summary of datasets by data source
SELECT d.data_source_label AS dataset_origin,
    COUNT(DISTINCT d.dataset_id) AS number_of_datasets,
    MIN(d.date_created) AS first_dataset,
    MAX(d.date_created) AS last_dataset,
    COALESCE(SUM(f_stats.file_count), 0) AS total_files,
    COALESCE(SUM(f_stats.total_bytes) / 1e9, 0) AS total_size_in_GB_non_zip_and_zip_files
FROM datasets d
    LEFT JOIN (
        SELECT dataset_id,
            COUNT(file_id) AS file_count,
            SUM(
                CASE
                    WHEN COALESCE(is_from_zip_file, 0) = 0 THEN size_in_bytes
                    ELSE 0
                END
            ) AS total_bytes
        FROM files
        GROUP BY dataset_id
    ) f_stats ON d.dataset_id = f_stats.dataset_id
GROUP BY d.data_source_label
ORDER BY d.data_source_label;