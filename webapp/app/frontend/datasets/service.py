from mdverse.database.database import (
    Author,
    Dataset,
    DatasetAuthorLink,
    DataSource,
    File,
)
from sqlalchemy import desc, func
from sqlalchemy.orm import selectinload
from sqlmodel import Session, or_, select


def get_all_datasets(session: Session) -> list[Dataset]:
    """
    Returns a list of all dataset objects, with their related objects loaded.
    """
    statement = select(Dataset).options(
        selectinload(Dataset.data_source),
        selectinload(Dataset.author),
    )
    results = session.exec(statement).all()
    return results


def get_all_datasets_for_datatables(
    session: Session,
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
            Dataset.file_number,
            Dataset.url_in_data_source.label("url"),
            func.group_concat(Author.name, "; ").label("author"),
        )
        .join(DataSource, Dataset.data_source_id == DataSource.data_source_id)
        .join(DatasetAuthorLink, Dataset.dataset_id == DatasetAuthorLink.dataset_id)
        .join(Author, DatasetAuthorLink.author_id == Author.author_id)
        .group_by(
            Dataset.dataset_id,
            DataSource.name,
            Dataset.id_in_data_source,
            Dataset.title,
            Dataset.description,
            Dataset.date_created,
            Dataset.file_number,
            Dataset.url_in_data_source,
        )
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
        statement = statement.where(
            or_(
                DataSource.name.ilike(f"%{search}%"),
                Dataset.id_in_data_source.ilike(f"%{search}%"),
                Dataset.title.ilike(f"%{search}%"),
                Dataset.description.ilike(f"%{search}%"),
                Author.name.ilike(f"%{search}%"),
            )
        )
    results = session.exec(statement).all()
    # df = pd.read_sql_query(statement, session.bind)
    # print(df.columns)
    return results


def get_dataset_info_by_id(session: Session, dataset_id: int) -> tuple[Dataset, int]:
    """
    Returns dataset from its id.
    """
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

    # Count how many total files are in the dataset.
    statement_total_files = select(
        func.count(File.file_id).label("total_all_files")
    ).where(File.dataset_id == dataset_id)

    dataset = session.exec(statement_dataset).first()
    total_files = session.exec(statement_total_files).first()
    return dataset, total_files


def get_all_files_from_dataset(session: Session, dataset_id: int) -> list[File]:
    """
    Returns a list of all files for a given dataset_id.
    """
    statement = (
        select(File)
        .options(
            selectinload(File.file_type),
            selectinload(File.dataset),
        )
        .where(File.dataset_id == dataset_id)
    )
    all_files = session.exec(statement).all()
    return all_files
