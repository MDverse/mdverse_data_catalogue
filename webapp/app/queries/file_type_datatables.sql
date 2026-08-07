-- Retrieve file information along with dataset details
SELECT f.name AS file_name,
    d.id_in_data_source AS dataset_id_in_origin,
    d.url_in_data_source AS dataset_url,
    d.data_source_label AS dataset_origin,
    COUNT(*) OVER() AS total_count
FROM files f
    JOIN datasets d ON f.dataset_id = d.dataset_id $where_sql
ORDER BY $order_col $sort_dir
LIMIT $limit OFFSET $offset;