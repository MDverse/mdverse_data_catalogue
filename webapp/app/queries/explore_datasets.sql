-- Search and paginate datasets
SELECT d.dataset_id,
    d.title,
    d.id_in_data_source,
    d.data_source_label,
    SUBSTRING(
        d.date_created
        FROM 1 FOR 10
    ) AS date_created,
    COUNT(DISTINCT f.file_id) AS files_count,
    COUNT(DISTINCT dp.publication_id) AS publications_count,
    COUNT(DISTINCT dm.ai_model_id) AS ai_models_count,
    COUNT(*) OVER() AS total_count
FROM datasets d
    LEFT JOIN files f ON d.dataset_id = f.dataset_id
    LEFT JOIN datasets_publications_link dp ON d.dataset_id = dp.dataset_id
    LEFT JOIN datasets_models_link dm ON d.dataset_id = dm.dataset_id $where_sql
GROUP BY d.dataset_id,
    d.title,
    d.id_in_data_source,
    d.data_source_label,
    d.date_created
ORDER BY $order_col $sort_dir
LIMIT $limit OFFSET $offset;