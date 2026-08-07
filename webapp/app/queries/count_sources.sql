-- Retrieve counts of various entities in the database
SELECT (
        SELECT COUNT(*)
        FROM datasets
    ) AS total_datasets,
    (
        SELECT COUNT(*)
        FROM files
    ) AS total_files,
    (
        SELECT COUNT(*)
        FROM publications
    ) AS total_publications,
    (
        SELECT COUNT(*)
        FROM ai_models
    ) AS total_ai_models,
    (
        SELECT COUNT(*)
        FROM data_sources
    ) AS total_sources,
    (
        SELECT COUNT(f.file_id)
        FROM files f
            JOIN file_types ft ON f.file_type_label = ft.file_type_label
        WHERE LOWER(ft.file_type_label) IN ('pdb', 'crd', 'gro', 'coor')
    ) AS total_structures,
    (
        SELECT COUNT(f.file_id)
        FROM files f
            JOIN file_types ft ON f.file_type_label = ft.file_type_label
        WHERE LOWER(ft.file_type_label) IN (
                'trr',
                'xtc',
                'dcd',
                'inpcrd',
                'dtr',
                'mdcrd',
                'nc',
                'ncdf',
                'trj'
            )
    ) AS total_trajectories;