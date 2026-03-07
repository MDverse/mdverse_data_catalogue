```mermaid
    erDiagram
        %% Relationships between tables and join tables
        authors ||--|{ datasets_authors_link : ""
        datasets ||--|{ files : ""
        datasets ||--o{ datasets_authors_link : ""
        datasets ||--o{ datasets_molecules_link : ""
        datasets }|--|| dataset_origins : ""
        datasets ||--o{ datasets_keywords_link : ""
        datasets ||--o{ datasets_software_link : ""
        keywords ||--|{ datasets_keywords_link : ""
        software ||--|{ datasets_software_link : ""
        files }|--o| files : ""
        files }|--|| file_types : ""
        files ||--o| topology_files : ""
        files ||--o| parameter_files : ""
        files ||--o| trajectory_files : ""
        topology_files ||--|{ molecules_topologies_link : ""
        parameter_files }|--o| thermostats : ""
        parameter_files }|--o| barostats : ""
        parameter_files }|--o| integrators : ""
        molecules ||--|{ molecules_topologies_link : ""
        molecules ||--|{ datasets_molecules_link : ""
        molecules }|--o| molecule_types : ""
        molecules ||--o{ molecules_external_db : ""
        molecules_external_db }|--o| databases : ""

        %% Join table definitions with descriptions
        datasets_authors_link {
            int dataset_id FK "Foreign key referencing datasets"
            int author_id FK "Foreign key referencing authors"
        }
        datasets_molecules_link {
            int dataset_id FK "Foreign key referencing datasets"
            int molecule_id FK "Foreign key referencing molecules"
        }
        datasets_keywords_link {
            int dataset_id FK "Foreign key referencing datasets"
            int keyword_id FK "Foreign key referencing keywords"
        }
        molecules_topologies_link {
            int molecule_id FK "Foreign key referencing molecules"
            int file_id FK "Foreign key referencing topology_files (the topology file)"
        }

        %% Table definitions with descriptions
        authors {
            int author_id PK "Primary key"
            str name UK "Author first name and last name"
            int orcid UK "ORCID identifier if provided"
        }
        datasets {
            int dataset_id PK "Primary key"
            int origin_id FK "Foreign key referencing dataset_origins "
            str id_in_origin "Dataset ID as provided by the original database"
            str doi "Digital Object Identifier for the dataset"
            str date_created "Creation date in the original database"
            str date_last_modified "Last modification date in the original database"
            str date_last_crawled "Last crawled date from the original source"
            int file_number "Number of files associated with the dataset"
            int download_number "Download count for the dataset"
            int view_number "View count for the dataset"
            str license "License information for the dataset"
            str url "URL leading to the dataset in its original database"
            str title "Title of the dataset"
            str description "Detailed description of the dataset"
        }
        files {
            int file_id PK
            int dataset_id FK "Foreign key referencing the owning dataset"
            str name "File name"
            int file_type_id FK "Foreign key referencing file_types"
            float size_in_bytes "File size in bytes"
            str md5 "MD5 checksum for file verification"
            str url "Direct URL to access the file ('None' if inside a zip archive)"
            bool is_from_zip_file "True if file is extracted from a zip archive"
            int parent_zip_file_id FK "File ID of the parent zip file, if applicable"
        }
        topology_files {
            int file_id PK, FK "Primary key and foreign key referencing files"
            int atom_number "Number of atoms in the topology system"
            bool has_protein "Indicates presence of protein residues"
            bool has_nucleic "Indicates presence of nucleic acid bases"
            bool has_lipid "Indicates presence of lipid residues"
            bool has_glucid "Indicates presence of glucid residues"
            bool has_water_ion "Indicates presence of water or ions"
        }
        parameter_files {
            int file_id PK, FK "Primary key and foreign key referencing files"
            float dt "Time step (in picoseconds) used in the simulation"
            int nsteps "Total number of simulation steps performed"
            float temperature "Temperature in Kelvin during simulation"
            int thermostat_id "Foreign key referencing thermostats"
            int barostat_id "Foreign key referencing barostats"
            int integrator_id "Foreign key referencing integrators"
        }
        trajectory_files {
            int file_id PK, FK "Primary key and foreign key referencing files"
            int atom_number "Number of atoms in the trajectory"
            int frame_number "Total Number of frames in the trajectory"
        }
        molecules {
            int molecule_id PK "Primary key"
            str name "of the molecule"
            str formula "Chemical formula of the molecule"
            str sequence "Sequence information (e.g., protein sequence)"
            int molecule_type_id FK "Foreign key referencing molecule_types"
        }
        molecules_external_db {
            int molecule_external_db_id PK "Primary key"
            int molecule_id FK "Foreign key referencing molecules"
            int database_id FK "Foreign key referencing databases"
            str id_in_external_db "Identifier of the molecule in the external database"
        }
        dataset_origins {
            int origin_id PK "Primary key"
            str name UK "Name of the origin"
        }
        databases {
            int database_id PK "Primary key"
            str name UK "Name of the database"
        }
        software {
            int software_id PK "Primary key"
            str name UK ""
            str version UK "Version of the software"
        }
        file_types {
            int file_type_id PK "Primary key"
            str name UK "Name of the file extension"
        }
        molecule_types {
            int molecule_type_id PK "Primary key"
            str name UK "Name of the molecule type"
        }
        barostats {
            int barostat_id PK "Primary key"
            str name UK "Possible barostat used. If no barostat is used, the value is 'no'. If the barostat is not listed in the documentation, the value is 'unknown'. If the barostat is not a string, the value is 'undefined'"
        }
        thermostats {
            int thermostat_id PK "Primary key"
            str name UK "Possible thermostat used. If no thermostat is used, the value is 'no'. If the thermostat is not listed in the documentation, the value is 'unknown'. If the thermostat is not a string, the value is 'undefined'"
        }
        integrators {
            int integrator_id PK "Primary key"
            str name UK "Algorithm used to integrate equations of motions"
        }
        keywords {
            int keyword_id PK "Primary key"
            str entry UK "Keyword entry"
        }
```