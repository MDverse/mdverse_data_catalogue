-- Search AI models and format parameter counts
SELECT m.ai_model_id,
    m.id_in_data_source AS name,
    m.tasks,
    m.number_of_parameters,
    ROUND(m.number_of_parameters / 1e9, 2) AS params_b,
    m.description,
    m.date_created AS created_on,
    m.url_in_data_source AS huggingface_url,
    COUNT(*) OVER() AS total_count
FROM ai_models m $where_sql
ORDER BY $order_col $sort_dir
LIMIT $limit OFFSET $offset;