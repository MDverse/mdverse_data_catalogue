-- Insert any new unique file types into file_types reference table before files ingestion
INSERT INTO file_types (file_type_label, comment)
SELECT DISTINCT 
    TRIM(file_type_label) AS file_type_label,
    NULL AS comment
FROM _stage_files
WHERE file_type_label IS NOT NULL 
  AND TRIM(file_type_label) != ''
ON CONFLICT (file_type_label) DO NOTHING;

-- Insert unique files into the main files table
INSERT INTO files (
    dataset_id,
    name,
    file_type_label,
    size_in_bytes,
    size_human_readable,
    md5,
    url,
    is_from_zip_file,
    parent_zip_file_id
)
SELECT
    d.dataset_id,
    s.name,
    TRIM(s.file_type_label),
    CAST(s.size_in_bytes AS DOUBLE),
    s.size_human_readable,
    s.md5,
    s.url,
    -- Compute boolean based on parent_zip_file_name presence
    CASE 
        WHEN s.parent_zip_file_name IS NOT NULL AND TRIM(s.parent_zip_file_name) != '' THEN TRUE 
        ELSE FALSE 
    END AS is_from_zip_file,
    NULL AS parent_zip_file_id
FROM _stage_files s
JOIN datasets d 
  ON d.id_in_data_source = CAST(s.dataset_id AS VARCHAR)
 AND d.data_source_label = s.data_source_label
LEFT JOIN files f
  ON f.dataset_id = d.dataset_id
 AND f.name = s.name
WHERE f.file_id IS NULL 
  AND s.file_type_label IS NOT NULL 
  AND TRIM(s.file_type_label) != '';

-- Update parent-child relationships for files contained inside zip archives
UPDATE files AS child
SET parent_zip_file_id = parent.file_id
FROM _stage_files s
JOIN datasets d
  ON d.id_in_data_source = CAST(s.dataset_id AS VARCHAR)
 AND d.data_source_label = s.data_source_label
JOIN files parent
  ON parent.dataset_id = d.dataset_id
 AND parent.name = s.parent_zip_file_name
WHERE child.dataset_id = d.dataset_id
  AND child.name = s.name
  AND s.parent_zip_file_name IS NOT NULL 
  AND TRIM(s.parent_zip_file_name) != '';