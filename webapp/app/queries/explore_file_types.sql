-- Aggregate file statistics by file type with pagination, dynamic search, and total count window
SELECT ft.file_type_label AS file_type,
    ft.comment AS comment,
    COUNT(f.file_id) AS number_of_files,
    COUNT(DISTINCT f.dataset_id) AS number_of_datasets,
    ROUND(SUM(f.size_in_bytes) / 1e9, 2) AS total_size_in_GB,
    COUNT(*) OVER() AS total_count
FROM file_types ft
    JOIN files f ON f.file_type_label = ft.file_type_label $where_sql
GROUP BY ft.file_type_label,
    ft.comment
ORDER BY $order_col $sort_dir
LIMIT $limit OFFSET $offset;