-- Search publications and calculate linked datasets and AI models
SELECT p.publication_id,
    p.title,
    p.journal,
    p.year,
    p.doi,
    p.url AS link,
    COUNT(DISTINCT dpl.dataset_id) AS linked_datasets_count,
    COUNT(DISTINCT pml.ai_model_id) AS linked_models_count,
    COUNT(*) OVER() AS total_count
FROM publications p
    LEFT JOIN datasets_publications_link dpl ON p.publication_id = dpl.publication_id
    LEFT JOIN publications_models_link pml ON p.publication_id = pml.publication_id $where_sql
GROUP BY p.publication_id,
    p.title,
    p.journal,
    p.year,
    p.doi,
    p.url
ORDER BY $order_col $sort_dir
LIMIT $limit OFFSET $offset;