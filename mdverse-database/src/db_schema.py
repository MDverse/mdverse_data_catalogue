from typing import Optional

from sqlmodel import Field, Relationship, SQLModel, UniqueConstraint, create_engine

# ============================================================================

"""Terminology:
Database = DB
FK = Foreign Key
PK = Primary Key
"""

""" Naming Conventions:
SQLModel uses python type hints to infer the database schema.
The following classes define the schema of the database.
Since we are using python syntax, it is important to note that:
- Class names are singular and in CamelCase.

We explicitly define the table name using the __tablename__ attribute.
This is done to ensure that the table name is in snake_case and plural form,
which is the convention in SQL databases (DBs).

Due to incompatibility issues between:
- SQLModel
- SQLAlchemy (that creates the engine for the database)
We will be writing the code without:
- using the "from __future__ import annotations" syntax
- using the "from typing import List" syntax
Therefore we will also be sure to use 'list["ClassName"]' or 'ClassName' when
defining certain relationships.
"""

# ============================================================================
# Many-to-Many (MtM) Link Tables
# ============================================================================

"""
Many to Many (MtM) relationships are represented using link tables in SQL DBs.
These tables contain FKs to the PKs of the tables that are related.

The link tables are named using the following convention:
- Class naming: Class1Class2Link
- Table naming: table1_table2_link

In the SQLModel documentation, these tables are referred to as "link tables".
However, in the context of SQL DBs, these tables are also known as:
- association table
- secondary table
- junction table
- intermediate table
- join table
- through table
- relationship table
- connection table
- cross-reference table
We will be using the term "link table" for coherence with the SQLModel doc.

Link tables/classes are always initialized at the beginning of the models.
This is necessary because, even with "from __future__ import annotations",
the class won't be recognized if "link_model = Class1Class2Link" is
used BEFORE the class is declared.
"""


class DatasetAuthorLink(SQLModel, table=True):
    """
    MtM link table between Dataset and Author
    LOGIC: A dataset can have many authors and
    an author can have published many datasets.
    """

    __tablename__ = "datasets_authors_link"

    dataset_id: Optional[int] = Field(
        default=None, foreign_key="datasets.dataset_id", primary_key=True
    )
    author_id: Optional[int] = Field(
        default=None, foreign_key="authors.author_id", primary_key=True
    )


class AuthorPaperLink(SQLModel, table=True):
    """
    MtM link table between Author and Paper
    LOGIC: An author can have written none or many papers
    (here an "author" can have written a paper or published a dataset) and,
    a paper is definitely written by one or many authors.
    """

    __tablename__ = "authors_papers_link"

    author_id: Optional[int] = Field(
        default=None, foreign_key="authors.author_id", primary_key=True
    )
    paper_id: Optional[int] = Field(
        default=None, foreign_key="papers.paper_id", primary_key=True
    )


# ============================================================================
# Main Tables
# ============================================================================

"""
Here we define the main tables of the database schema.
By "main tables" we mean the ones that represent the main entities in the DB.

These tables are those that have the most attributes
and relationships with other tables.
"""


class Dataset(SQLModel, table=True):
    __tablename__ = "datasets"

    # Attributes/Table columns -----------------------------------------------
    dataset_id: Optional[int] = Field(default=None, primary_key=True)
    data_source_id: int = Field(foreign_key="data_sources.data_source_id")
    id_in_data_source: str
    url_in_data_source: Optional[str] = Field(default=None)
    project_id: Optional[int] = Field(default=None, foreign_key="projects.project_id")
    id_in_project: Optional[str] = Field(default=None)
    url_in_project: Optional[str] = Field(default=None)
    doi: Optional[str] = Field(default=None)
    date_created: Optional[str] = None  # YYYY-MM-DD format
    date_last_modified: Optional[str] = None  # YYYY-MM-DD format
    date_last_crawled: str  # ("%Y-%m-%dT%H:%M:%S")
    file_number: int = 0
    download_number: int = 0
    view_number: int = 0
    license: Optional[str] = Field(default=None)
    title: str
    description: Optional[str] = None
    keywords: Optional[str] = None

    # Relationships: files, data_sources, authors, projects, annotations -----

    # A dataset can have many files, authors, keywords, software and molecules
    # (although it can have zero molecules)
    # A dataset can have only one origin (not a list)
    file: list["File"] = Relationship(back_populates="dataset", cascade_delete=True)
    data_source: "DataSource" = Relationship(back_populates="dataset")
    author: Optional[list["Author"]] = Relationship(
        back_populates="dataset", link_model=DatasetAuthorLink
    )
    project: Optional["Project"] = Relationship(back_populates="dataset")
    annotation: Optional[list["Annotation"]] = Relationship(
        back_populates="dataset"
    )


class File(SQLModel, table=True):
    __tablename__ = "files"

    # Attributes/Table columns -----------------------------------------------
    file_id: Optional[int] = Field(default=None, primary_key=True)
    dataset_id: int = Field(foreign_key="datasets.dataset_id", ondelete="CASCADE")
    name: str
    file_type_id: int = Field(foreign_key="file_types.file_type_id")
    size_in_bytes: Optional[float] = Field(default=None)
    # files that belong to a zip file don't have md5
    md5: Optional[str] = Field(default=None)
    # files that belong to a zip file don't have url
    url: Optional[str] = Field(default=None)
    is_from_zip_file: bool = Field(index=True)
    parent_zip_file_id: Optional[int] = Field(
        # notice the lowercase "f" to refer to the database table name
        foreign_key="files.file_id",
        default=None,
        nullable=True,
    )

    # Relationships: datasets, files, topology_files, parameter_files, -------
    # trajectory_files, file_types

    parent: Optional["File"] = Relationship(
        back_populates="children",
        sa_relationship_kwargs=dict(remote_side="File.file_id"),
    )  # notice the uppercase "F" to refer to this table class
    children: list["File"] = Relationship(back_populates="parent")

    dataset: Dataset = Relationship(back_populates="file")
    topology_file: Optional["TopologyFile"] = Relationship(back_populates="file", cascade_delete=True)
    parameter_file: Optional["ParameterFile"] = Relationship(back_populates="file", cascade_delete=True)
    trajectory_file: Optional["TrajectoryFile"] = Relationship(back_populates="file", cascade_delete=True)
    file_type: "FileType" = Relationship(back_populates="file")
    annotation: Optional[list["Annotation"]] = Relationship(
        back_populates="file", cascade_delete=True
    )


class Author(SQLModel, table=True):
    __tablename__ = "authors"
    __table_args__ = (UniqueConstraint("name", "orcid"),)

    # Attributes/Table columns -----------------------------------------------
    author_id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    orcid: Optional[str] = Field(default=None)

    # Relationships: dataset
    dataset: list[Dataset] = Relationship(
        back_populates="author", link_model=DatasetAuthorLink
    )


class Annotation(SQLModel, table=True):
    __tablename__ = "annotations"

    # Attributes/Table columns -----------------------------------------------
    annotation_id: Optional[int] = Field(default=None, primary_key=True)
    dataset_id: int = Field(foreign_key="datasets.dataset_id")
    provenance_type_id: int = Field(foreign_key="provenance_types.provenance_id")
    annotation_type_id: int = Field(foreign_key="annotation_types.annotation_type_id")
    file_id: Optional[int] = Field(foreign_key="files.file_id", default=None)
    paper_id: Optional[int] = Field(foreign_key="papers.paper_id", default=None)
    value: str
    quality_score: Optional[str] = Field(default=None)
    value_extra: Optional[str] = Field(default=None)
    comment: Optional[str] = Field(default=None)

    # Relationships: datasets, provenance_types, annotation_types, -----------
    # files, papers

    dataset: "Dataset" = Relationship(back_populates="annotation")
    provenance_type: Optional["ProvenanceType"] = Relationship(
        back_populates="provenance")
    annotation_type: Optional["AnnotationType"] = Relationship(
        back_populates="annotation")
    file: Optional["File"] = Relationship(back_populates="annotation")
    paper: Optional["Paper"] = Relationship(back_populates="annotation")
    molecule: Optional["Molecule"] = Relationship(
        back_populates="annotation", cascade_delete=True
    )


class Paper(SQLModel, table=True):
    __tablename__ = "papers"

    # Attributes/Table columns -----------------------------------------------
    paper_id: Optional[int] = Field(default=None, primary_key=True)
    doi: Optional[str] = Field(default=None)
    title: str
    abstract: Optional[str] = Field(default=None)
    journal: str
    url: Optional[str] = Field(default=None)
    year: Optional[str] = None  # YYYY
    keywords: Optional[str] = None

    # Relationships: authors, annotations

    annotation: Optional[list[Annotation]] = Relationship(
        back_populates="paper", cascade_delete=True
    )


class Project(SQLModel, table=True):
    __tablename__ = "projects"

    # Attributes/Table columns -----------------------------------------------
    project_id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True)
    url: str
    comment: Optional[str] = Field(default=None)
    citation: Optional[str] = Field(default=None)

    # Relationships: datasets
    dataset: list["Dataset"] = Relationship(back_populates="project")


class Molecule(SQLModel, table=True):
    __tablename__ = "molecules"

    # Attributes/Table columns -----------------------------------------------
    molecule_id: Optional[int] = Field(default=None, primary_key=True)
    annotation_id: int = Field(foreign_key="annotations.annotation_id")
    name: str
    formula: str
    sequence: str
    molecule_type_id: Optional[int] = Field(
        foreign_key="molecule_types.molecule_type_id"
    )

    # Relationships: annotations, molecule_types, molecules_external_db ------
    annotation: "Annotation" = Relationship(back_populates="molecule")
    mol_ext_db: Optional[list["MoleculeExternalDb"]] = Relationship(
        back_populates="molecule"
    )
    molecule_type: Optional["MoleculeType"] = Relationship(back_populates="molecule")


class MoleculeExternalDb(SQLModel, table=True):
    __tablename__ = "molecules_external_db"

    # Attributes/Table columns -----------------------------------------------
    mol_ext_db_id: Optional[int] = Field(default=None, primary_key=True)
    molecule_id: int = Field(foreign_key="molecules.molecule_id")
    db_name: str = Field(index=True)
    id_in_external_db: str
    database_id: Optional[int] = Field(foreign_key="databases.database_id")

    # Relationships: molecules, databases ------------------------------------

    molecule: Molecule = Relationship(back_populates="mol_ext_db")
    database: Optional["Database"] = Relationship(back_populates="mol_ext_db")


# ============================================================================
# Simulation Files Tables
# ============================================================================

"""
These tables correspond to the files that are used in molecular simulations.
The tables are named after the file types:
- Topology files
- Parameter files
- Trajectory files

These tables have a one-to-one relationship with the `Files` table.
This means that each record in these tables
corresponds to exactly one record in the `Files` table.
This is why in these tables, the `file_id` is both the PK and a FK.
"""


class TopologyFile(SQLModel, table=True):
    __tablename__ = "topology_files"

    # File id is both the PK but also a FK to the Files table
    file_id: Optional[int] = Field(
        default=None, primary_key=True, foreign_key="files.file_id", ondelete="CASCADE"
    )
    atom_number: int
    has_protein: bool
    has_nucleic: bool
    has_lipid: bool
    has_glucid: bool
    has_water_ion: bool

    # Relationships: files, molecules
    file: File = Relationship(back_populates="topology_file")


class ParameterFile(SQLModel, table=True):
    __tablename__ = "parameter_files"

    file_id: Optional[int] = Field(
        default=None, primary_key=True, foreign_key="files.file_id", ondelete="CASCADE"
    )
    dt: Optional[float] = Field(default=None)
    nsteps: Optional[int] = Field(default=None)
    temperature: Optional[float] = Field(default=None)
    thermostat: Optional[str] = Field(default=None)
    barostat: Optional[str] = Field(default=None)
    integrator: Optional[str] = Field(default=None)

    # Relationships: files, thermostats, barostats, integrators
    file: File = Relationship(back_populates="parameter_file")


class TrajectoryFile(SQLModel, table=True):
    __tablename__ = "trajectory_files"

    file_id: Optional[int] = Field(
        default=None, primary_key=True, foreign_key="files.file_id", ondelete="CASCADE"
    )
    atom_number: int
    frame_number: int

    # Relationships: files
    file: File = Relationship(back_populates="trajectory_file")


# ============================================================================
# "Type" Tables
# ============================================================================

"""
These tables are used to store the, what we call,
"type" of the entities in the database.

For example, the different types of files that are used in molecular
simulations, the different types of molecules, etc.

These tables have a one-to-many relationship with the main tables.
"""


class FileType(SQLModel, table=True):
    __tablename__ = "file_types"

    file_type_id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True)
    comment: Optional[str] = Field(default=None)

    # Relationships: files
    file: list[File] = Relationship(back_populates="file_type")


class MoleculeType(SQLModel, table=True):
    __tablename__ = "molecule_types"

    molecule_type_id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True)
    comment: Optional[str] = Field(default=None)

    # Relationships: molecules
    molecule: list[Molecule] = Relationship(back_populates="molecule_type")


class Database(SQLModel, table=True):
    __tablename__ = "databases"

    database_id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True)
    url: Optional[str] = Field(default=None)
    comment: Optional[str] = Field(default=None)

    # Relationships: molecules_external_db
    mol_ext_db: list[MoleculeExternalDb] = Relationship(back_populates="database")


class DataSource(SQLModel, table=True):
    __tablename__ = "data_sources"

    data_source_id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True)
    url: Optional[str] = Field(default=None)
    citation: Optional[str] = Field(default=None)
    comment: Optional[str] = Field(default=None)

    # Relationships: datasets
    dataset: list[Dataset] = Relationship(back_populates="data_source")


class ProvenanceType(SQLModel, table=True):
    __tablename__ = "provenance_types"

    provenance_id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True)
    comment: Optional[str] = Field(default=None)

    # Relationships: annotations
    provenance: list[Annotation] = Relationship(back_populates="provenance_type")


class AnnotationType(SQLModel, table=True):
    __tablename__ = "annotation_types"
    __table_args__ = (UniqueConstraint("name", "label"),)

    annotation_type_id: Optional[int] = Field(default=None, primary_key=True)
    label: str
    name: str
    comment: Optional[str] = Field(default=None)

    # Relationships: annotations
    annotation: list[Annotation] = Relationship(back_populates="annotation_type")


# ============================================================================
# Engine
# ============================================================================

sqlite_file_name = "database.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

engine = create_engine(sqlite_url)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)
