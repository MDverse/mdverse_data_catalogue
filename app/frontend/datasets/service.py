from sqlalchemy import extract, func, case, desc
from sqlalchemy.orm import selectinload, aliased
from sqlmodel import Session, select, or_, col


from ...db_schema import (
    Dataset,
    DataSource,
    File,
    FileType,
    ParameterFile,
    TopologyFile,
    TrajectoryFile,
    engine,
)

def get_all_datasets() -> list[Dataset]:
    """
    Returns a list of all dataset objects, with their related objects loaded.
    """
    with Session(engine) as session:
        statement = (
            select(Dataset)
            .options(
                selectinload(Dataset.data_source),
                selectinload(Dataset.author),
            )
        )
        results = session.exec(statement).all()
        return results

def get_all_datasets_for_datatables(
        sort_column_name: str | None = None,
        sort_direction: str | None = "asc",
        start: int | None = None,
        length: int | None = None,
        search: str | None = None,
    ) -> list[Dataset]:
    """
    Returns a list of all dataset, with their related fields.
    """
    statement = (
        select(
            DataSource.name.label("dataset_origin"),
            Dataset.id_in_data_source,
            Dataset.dataset_id,
            Dataset.title,
            Dataset.description,
            Dataset.date_created,
            Dataset.date_last_modified,
            Dataset.file_number,
            Dataset.download_number,
            Dataset.view_number,
            Dataset.url_in_data_source.label("url"),
        )
        .join(DataSource, Dataset.data_source_id == DataSource.data_source_id)
    )

    if sort_column_name is not None:
        if sort_direction == "asc":
            statement = statement.order_by(sort_column_name)
        elif sort_direction == "desc":
            statement = statement.order_by(desc(sort_column_name))

    if start is not None:
        statement = statement.offset(start)

    if length is not None:
        statement = statement.limit(length)

    if search is not None:
        statement = statement.where(or_(
            DataSource.name.ilike(f"%{search}%"),
            Dataset.id_in_data_source.ilike(f"%{search}%"),
            Dataset.title.ilike(f"%{search}%"),
            Dataset.description.ilike(f"%{search}%"),
        ))
    with Session(engine) as session:
        results = session.exec(statement).all()
        # df = pd.read_sql_query(statement, session.bind)
        # print(df.columns)
        return results

def get_dataset_info_by_id(dataset_id: int):
    """
    Returns dataset from its id.
    """
    with Session(engine) as session:
        statement_dataset = (
            select(Dataset)
            .options(
                # Load the related origin object so that dataset.origin is available.
                selectinload(Dataset.data_source),
                # Load the many-to-many relationship for authors
                selectinload(Dataset.author),
            )
            .where(Dataset.dataset_id == dataset_id)
        )

        # Count how many total files, topology, parameter, and
        # trajectory files are in the dataset
        statement_total_files = (
            select(
            func.count(File.file_id).label("total_all_files"),
            func.count(File.file_id).filter(FileType.name == "gro").label("total_topology_files"),
            func.count(File.file_id).filter(FileType.name == "mdp").label("total_parameter_files"),
            func.count(File.file_id).filter(FileType.name == "xtc").label("total_trajectory_files"),
            )
            .join(FileType, File.file_type_id == FileType.file_type_id)
            .where(File.dataset_id == dataset_id)
        )

        # Count how many files have been analysed for this dataset,
        # a.k.a. how many are actually in the tables
        statement_analysed_files = select(
            (select(func.count(TopologyFile.file_id))
            .select_from(TopologyFile)
            .join(File, File.file_id == TopologyFile.file_id, isouter=True)
            .where(File.dataset_id == dataset_id)
            ).label("analysed_topology_files"),
            (select(func.count(ParameterFile.file_id))
            .select_from(ParameterFile)
            .join(File, File.file_id == ParameterFile.file_id, isouter=True)
            .where(File.dataset_id == dataset_id)
            ).label("analysed_parameter_files"),
            (select(func.count(TrajectoryFile.file_id))
            .select_from(TrajectoryFile)
            .join(File, File.file_id == TrajectoryFile.file_id, isouter=True)
            .where(File.dataset_id == dataset_id)
            ).label("analysed_trajectory_files")
        )

        result_dataset = session.exec(statement_dataset).first()
        result_total_files = session.exec(statement_total_files).first()
        result_analysed_files = session.exec(statement_analysed_files).first()

        return result_dataset, result_total_files, result_analysed_files

def get_all_files_from_dataset(dataset_id: int) -> list[File]:
    """
    Returns a list of all files for a given dataset_id.
    """
    with Session(engine) as session:
        statement = (
            select(File)
            .options(
                selectinload(File.file_type),
                selectinload(File.dataset),
            )
            .where(File.dataset_id == dataset_id)
        )
        results = session.exec(statement).all()
        return results