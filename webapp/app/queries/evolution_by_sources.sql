-- Retrieve the number of datasets and files per data source and year
SELECT d.data_source_label AS repository,
    CAST(
        EXTRACT(
            year
            FROM TRY_CAST(d.date_created AS DATE)
        ) AS INT
    ) AS year,
    COUNT(DISTINCT d.dataset_id) AS datasets_count,
    COUNT(f.file_id) AS files_count
FROM datasets d
    LEFT JOIN files f ON d.dataset_id = f.dataset_id
WHERE d.date_created IS NOT NULL
    AND d.date_created != ''
GROUP BY d.data_source_label,
    year
HAVING year IS NOT NULL
    AND year >= 2012
ORDER BY repository,
    year;