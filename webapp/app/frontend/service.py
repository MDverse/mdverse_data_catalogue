from datetime import datetime

import numpy as np
from bokeh.models import ColumnDataSource, NumeralTickFormatter
from bokeh.palettes import d3
from bokeh.plotting import figure
from mdverse.database.database import (
    Dataset,
    DataSource,
    File,
    FileType,
)
from sqlalchemy import extract, func
from sqlmodel import Session, select

# ============================================================================
# Queries for index.html
# ============================================================================


def get_dataset_origin_summary(session: Session) -> tuple[list[any], dict[str, str]]:
    """
    Returns rows grouped by dataset origin, with columns:
        dataset_origin,
        number_of_datasets,
        first_dataset,
        last_dataset,
        files (top-level),
        total_size_in_GB_non_zip_and_zip_files,
        zip_files (parent files that are zip),
        files_within_zip_files,
        total_files
    """
    statement = (
        select(
            # Dataset stats
            DataSource.name.label("dataset_origin"),
            func.count(func.distinct(Dataset.dataset_id)).label("number_of_datasets"),
            func.min(Dataset.date_created).label("first_dataset"),
            func.max(Dataset.date_created).label("last_dataset"),
            # Sum size of files that has is_from_zip_file == False
            (
                func.sum(File.size_in_bytes).filter(File.is_from_zip_file == False)  # noqa: E712
                / 1e9
            ).label("total_size_in_GB_non_zip_and_zip_files"),
            # Total files (outside-non-zip_parent + parent zips + inside zips)
            func.count(func.distinct(File.file_id)).label("total_files"),
        )
        .join(Dataset, Dataset.data_source_id == DataSource.data_source_id)
        # We have to do an outerjoin here because
        # there are some datasets with no files
        # For example some osf datasets have no files
        .outerjoin(File, File.dataset_id == Dataset.dataset_id)
        .outerjoin(FileType, File.file_type_id == FileType.file_type_id)
        .group_by(DataSource.name)
    )

    datasets_stats_results = session.exec(statement).all()

    datasets_stats_total_count = {
        "number_of_datasets": "{:,}".format(
            sum(row.number_of_datasets for row in datasets_stats_results)
        ),
        "first_dataset": min(
            row.first_dataset for row in datasets_stats_results if row.first_dataset
        )
        if any(row.first_dataset for row in datasets_stats_results)
        else None,
        "last_dataset": max(
            row.last_dataset for row in datasets_stats_results if row.last_dataset
        )
        if any(row.last_dataset for row in datasets_stats_results)
        else None,
        "total_files": "{:,}".format(
            sum(row.total_files for row in datasets_stats_results)
        ),
        "total_size_in_GB_non_zip_and_zip_files": "{:,.0f}".format(
            sum(
                row.total_size_in_GB_non_zip_and_zip_files
                for row in datasets_stats_results
            )
        ),
    }
    # Format date
    for item in ["first_dataset", "last_dataset"]:
        if datasets_stats_total_count[item]:
            datasets_stats_total_count[item] = datasets_stats_total_count[item].split(
                "T"
            )[0]

    statement_topologies = (
        select(func.count(File.file_id))
        .join(FileType, File.file_type_id == FileType.file_type_id)
        .where(FileType.name.in_(["pdb", "crd", "gro", "coor"]))
    )
    results_topologies = session.exec(statement_topologies).first()

    statement_trajectories = (
        select(func.count(File.file_id))
        .join(FileType, File.file_type_id == FileType.file_type_id)
        .where(
            FileType.name.in_(
                ["trr", "xtc", "dcd", "inpcrd", "dtr", "mdcrd", "nc", "ncdf", "trj"]
            )
        )
    )
    results_trajectories = session.exec(statement_trajectories).first()

    statement_sources = select(func.count()).select_from(DataSource)
    result_sources = session.exec(statement_sources).first()

    home_page_banner_stats = {
        "total_topology_files": "{:,}".format(results_topologies)
        if results_topologies
        else "0",
        "total_trajectory_files": "{:,}".format(results_trajectories)
        if results_trajectories
        else "0",
        "total_data_sources": "{:,}".format(result_sources) if result_sources else "0",
    }

    return datasets_stats_results, datasets_stats_total_count, home_page_banner_stats


def get_files_yearly_counts_for_origin(session: Session, origin_name: str):
    statement = (
        select(
            extract("year", Dataset.date_created).label("year"),
            func.count(Dataset.dataset_id).label("count"),
        )
        .join(File, File.dataset_id == Dataset.dataset_id)
        .join(DataSource, Dataset.data_source_id == DataSource.data_source_id)
        .where(DataSource.name == origin_name)
        .group_by("year")
        .order_by("year")
    )
    results = session.exec(statement).all()
    return {int(row.year): row.count for row in results if row.year is not None}


def extract_data_repository_names(session: Session):
    statement = select(DataSource.name).distinct()
    return session.exec(statement).all()


def create_files_plot(session: Session):
    repository_names = extract_data_repository_names(session)
    all_years = list(range(2012, datetime.now().year + 1))

    data = {
        "year": [str(year) for year in all_years],
    }
    for repository in repository_names:
        stats = get_files_yearly_counts_for_origin(session, repository)
        data[repository] = [stats.get(year, 0) for year in all_years]

    source = ColumnDataSource(data=data)

    plot = figure(
        x_range=data["year"],
        height=600,
        width=1000,
        title="Number of files per year per data repository",
        tooltips=[
            ("Year", "@year"),
            ("Data repository", "$name"),
            ("Number of files", "@$name{0,0}"),
        ],
        background_fill_color="#fafafa",
    )

    plot.toolbar.active_drag = None

    plot.vbar_stack(
        stackers=repository_names,
        x="year",
        width=0.8,
        source=source,
        color=d3["Category20"][len(repository_names)],
        legend_label=repository_names,
    )
    plot.xaxis.axis_label = "Year"
    plot.yaxis.axis_label = "Number of files"
    plot.yaxis.formatter = NumeralTickFormatter(format="0,0")

    plot.title.text_font_size = "14pt"
    plot.xaxis.axis_label_text_font_size = "12pt"
    plot.yaxis.axis_label_text_font_size = "12pt"
    plot.xaxis.major_label_text_font_size = "10pt"
    plot.yaxis.major_label_text_font_size = "10pt"

    plot.legend.location = "top_left"
    plot.legend.background_fill_alpha = 0.3
    plot.legend.border_line_color = None
    plot.legend.label_text_font_size = "10pt"

    return plot


# Similarly, create a plot for datasets per year.
def get_dataset_yearly_counts_for_origin(session: Session, origin_name: str):
    statement = (
        select(
            extract("year", Dataset.date_created).label("year"),
            func.count(Dataset.dataset_id).label("count"),
        )
        .join(DataSource, Dataset.data_source_id == DataSource.data_source_id)
        .where(DataSource.name == origin_name)
        .group_by("year")
        .order_by("year")
    )
    results = session.exec(statement).all()
    return {int(row.year): row.count for row in results if row.year is not None}


def create_datasets_plot(session: Session):
    """Create a line plot with cumulative number of datasets per year.

    One line per data repository.

    Doc:
    - https://docs.bokeh.org/en/latest/docs/user_guide/interaction/tools.html#ug-interaction-tools-hover-tool
    - https://docs.bokeh.org/en/latest/docs/user_guide/interaction/legends.html
    """
    repository_names = extract_data_repository_names(session)
    all_years = list(range(2012, datetime.now().year + 1))

    data = {
        "year": [str(year) for year in all_years],
    }
    for repository in repository_names:
        stats = get_dataset_yearly_counts_for_origin(session, repository)
        counts = np.array([stats.get(year, 0) for year in all_years])
        data[repository] = np.cumsum(counts)

    source = ColumnDataSource(data=data)
    colors = d3["Category20"][len(repository_names)]

    plot = figure(
        x_range=data["year"],
        y_axis_type="log",
        height=600,
        width=1000,
        title="Cumulative number of datasets per year per data repository",
        tooltips=[
            ("Year", "@year"),
            ("Data repository", "$name"),
            ("Number of datasets", "$snap_y{0,0}"),
        ],
        background_fill_color="#fafafa",
    )

    for idx, repository in enumerate(repository_names):
        plot.line(
            x="year",
            y=repository,
            width=2,
            source=source,
            color=colors[idx],
            legend_label=repository,
            name=repository,
        )
        plot.scatter(
            x="year",
            y=repository,
            size=8,
            source=source,
            fill_color="white",
            color=colors[idx],
            legend_label=repository,
            name=repository,
        )
    plot.legend.click_policy = "hide"
    plot.toolbar.active_drag = None

    plot.xaxis.axis_label = "Year"
    plot.yaxis.axis_label = "Number of datasets"
    plot.yaxis.formatter = NumeralTickFormatter(format="0,0")

    plot.title.text_font_size = "14pt"
    plot.xaxis.axis_label_text_font_size = "12pt"
    plot.yaxis.axis_label_text_font_size = "12pt"
    plot.xaxis.major_label_text_font_size = "10pt"
    plot.yaxis.major_label_text_font_size = "10pt"

    plot.legend.location = "top_left"
    plot.legend.background_fill_alpha = 0.3
    plot.legend.border_line_color = None
    plot.legend.label_text_font_size = "10pt"

    return plot
