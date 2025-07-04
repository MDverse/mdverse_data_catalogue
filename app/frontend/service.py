from pathlib import Path

import time
from datetime import timedelta

import pandas as pd
import matplotlib.pyplot as plt
from bokeh.models import ColumnDataSource, NumeralTickFormatter
from bokeh.plotting import figure
from sqlalchemy import extract, func, case, desc
from sqlalchemy.orm import selectinload, aliased
from sqlmodel import Session, select, or_, col
from wordcloud import STOPWORDS, WordCloud
from typing import Optional

from ..db_schema import (
    Dataset,
    DataSource,
    File,
    FileType,
    ParameterFile,
    TopologyFile,
    TrajectoryFile,
    engine,
)


# ============================================================================
# Queries for index.html
# ============================================================================


def get_dataset_origin_summary() -> tuple[list[any], dict[str, str]]:
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
    with Session(engine) as session:
        statement = (
            select(
                # Dataset stats
                DataSource.name.label("dataset_origin"),
                func.count(func.distinct(Dataset.dataset_id)).label("number_of_datasets"),
                func.min(Dataset.date_created).label("first_dataset"),
                func.max(Dataset.date_created).label("last_dataset"),

                # Count files that are not zip files, also is_from_zip_file == False
                func.count(func.distinct(File.file_id))
                   .filter(
                       (File.is_from_zip_file == False),
                       (FileType.name != "zip")
                       )
                   .label("non_zip_files"),

                # Sum size of files that has is_from_zip_file == False
                (func.sum(File.size_in_bytes).filter(File.is_from_zip_file == False)/ 1e9
                ).label("total_size_in_GB_non_zip_and_zip_files"),

                # Count parent zip files (FileType.name == 'zip')
                func.count(func.distinct(File.file_id))
                   .filter(
                       (File.is_from_zip_file == False) &
                       (FileType.name == "zip")
                   )
                   .label("zip_files"),

                # Count files that are inside zip files
                func.count(func.distinct(File.file_id))
                   .filter(File.is_from_zip_file)
                   .label("files_within_zip_files"),

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
            "number_of_datasets": "{:,}".format(sum(
            row.number_of_datasets for row in datasets_stats_results
            )),
            "first_dataset": min(
            row.first_dataset for row in datasets_stats_results
            ) if any(row.first_dataset for row in datasets_stats_results) else None,
            "last_dataset": max(
            row.last_dataset for row in datasets_stats_results
            ) if any(row.last_dataset for row in datasets_stats_results) else None,
            "non_zip_files": "{:,}".format(sum(
            row.non_zip_files for row in datasets_stats_results
            )),
            "zip_files": "{:,}".format(sum(
            row.zip_files for row in datasets_stats_results
            )),
            "files_in_zip_files": "{:,}".format(sum(
            row.files_within_zip_files for row in datasets_stats_results
            )),
            "total_files": "{:,}".format(sum(
            row.total_files for row in datasets_stats_results
            )),
            "total_size_in_GB_for_non_zip_and_zip_files ": "{:,.0f}".format(sum(
            row.total_size_in_GB_non_zip_and_zip_files for row in datasets_stats_results
            )),
        }


        statement_topologies = (
            select(func.count(File.file_id))
            .join(FileType, File.file_type_id == FileType.file_type_id)
            .where(FileType.name.in_(["pdb", "crd", "gro", "coor"]))
        )
        results_toplogies = session.exec(statement_topologies).first()

        statement_trajectories = (
            select(func.count(File.file_id))
            .join(FileType, File.file_type_id == FileType.file_type_id)
            .where(FileType.name.in_(["trr", "xtc", "dcd", "inpcrd", "dtr", "mdcrd", "nc", "ncdf", "trj"]))
        )
        results_trajectories = session.exec(statement_trajectories).first()
        
        statement_sources = select(func.count()).select_from(DataSource)
        result_sources = session.exec(statement_sources).first()

        home_page_banner_stats = {
            "total_topology_files": "{:,}".format(results_toplogies) if results_toplogies else "0",
            "total_trajectory_files": "{:,}".format(results_trajectories) if results_trajectories else "0",
            "total_data_sources": "{:,}".format(result_sources) if result_sources else "0",
        }

        return datasets_stats_results, datasets_stats_total_count, home_page_banner_stats


def get_titles():
    with Session(engine) as session:
        statement = select(Dataset.title)
        titles = session.exec(statement).all()
    return titles


def generate_title_wordcloud():
    wordcloud_path = Path("static/wordcloud.png")

    # Check if the file already exists
    if wordcloud_path.exists():
        print("Wordcloud already exists, skipping generation.")
        return

    # Retrieve dataset titles using the query function
    titles = get_titles()  # e.g. ['Dataset Title 1', 'Dataset Title 2', ...]

    # Join all titles into a single string (space separated)
    text = " ".join(titles)

    # Define custom stopwords to remove unwanted words (adjust as needed)
    custom_stopwords = set(STOPWORDS)
    custom_stopwords.update([
        "none",
        "and",
        "of",
        "with",
        ",",
        "which",
        "incl",
        "from",
        "the",
        "a",
        "an",
        "for",
        "on",
        "in",
        "to",
        "by",
        "as",
        ])

    # Create the WordCloud object with the desired resolution settings
    wordcloud = WordCloud(
        width=1600,
        height=800,
        background_color="white",
        stopwords=custom_stopwords
    ).generate(text)

    # Create the figure and display the wordcloud image
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")

    # Save the wordcloud image with a high DPI for better resolution
    plt.savefig(wordcloud_path, dpi=500, bbox_inches="tight")
    print(f"Wordcloud saved as {wordcloud_path}")


def get_files_yearly_counts_for_origin(session: Session, origin_name: str):
    stmt = (
        select(
            extract('year', Dataset.date_created).label('year'),
            func.count(Dataset.dataset_id).label('count')
        )
        .join(File, File.dataset_id == Dataset.dataset_id)
        .join(DataSource, Dataset.data_source_id == DataSource.data_source_id)
        .where(DataSource.name == origin_name)
        .group_by('year')
        .order_by('year')
    )
    results = session.exec(stmt).all()
    return {int(row.year): row.count for row in results if row.year is not None}


def create_files_plot():
    with Session(engine) as session:
        zenodo_data = get_files_yearly_counts_for_origin(session, "zenodo")
        osf_data = get_files_yearly_counts_for_origin(session, "osf")
        figshare_data = get_files_yearly_counts_for_origin(session, "figshare")

    all_years = sorted(
        set(zenodo_data.keys()) | set(osf_data.keys()) | set(figshare_data.keys())
    )

    data = {
        'year': [str(y) for y in all_years],
        'Zenodo': [zenodo_data.get(y, 0) for y in all_years],
        'OSF': [osf_data.get(y, 0) for y in all_years],
        'Figshare': [figshare_data.get(y, 0) for y in all_years]
    }

    source = ColumnDataSource(data=data)
    repositories = ["Zenodo", "OSF", "Figshare"]
    colors = ['#66c2a5', '#fc8d62', '#8da0cb']

    p = figure(
        x_range=data['year'],
        height=500,
        width=800,
        title="Number of files per year per data repository",
        tooltips=[
            ("Year", "@year"),
            ("Data repository", "$name"),
            ("Number of files", "@$name{0,0}")
        ],
        background_fill_color="#fafafa",
    )

    p.toolbar.active_drag = None

    p.vbar_stack(
        stackers=repositories,
        x="year",
        width=0.8,
        source=source,
        color=colors,
        legend_label=repositories
    )
    p.xaxis.axis_label="Year"
    p.yaxis.axis_label="Number of files"
    p.yaxis.formatter=NumeralTickFormatter(format="0,0")

    p.title.text_font_size = "14pt"
    p.xaxis.axis_label_text_font_size = "12pt"
    p.yaxis.axis_label_text_font_size = "12pt"
    p.xaxis.major_label_text_font_size = "10pt"
    p.yaxis.major_label_text_font_size = "10pt"

    p.legend.location = "top_left"
    p.legend.background_fill_alpha = 0.3
    p.legend.border_line_color = None
    p.legend.label_text_font_size = "10pt"

    return p


# Similarly, create a plot for datasets per year.
def get_dataset_yearly_counts_for_origin(session: Session, origin_name: str):
    stmt = (
        select(
            extract('year', Dataset.date_created).label('year'),
            func.count(Dataset.dataset_id).label('count')
        )
        .join(DataSource, Dataset.data_source_id == DataSource.data_source_id)
        .where(DataSource.name == origin_name)
        .group_by('year')
        .order_by('year')
    )
    results = session.exec(stmt).all()
    return {int(row.year): row.count for row in results if row.year is not None}


def create_datasets_plot():
    with Session(engine) as session:
        zenodo_data = get_dataset_yearly_counts_for_origin(session, "zenodo")
        osf_data = get_dataset_yearly_counts_for_origin(session, "osf")
        figshare_data = get_dataset_yearly_counts_for_origin(session, "figshare")

    all_years = sorted(
        set(zenodo_data.keys()) | set(osf_data.keys()) | set(figshare_data.keys())
    )

    data = {
        'year': [str(y) for y in all_years],
        'Zenodo': [zenodo_data.get(y, 0) for y in all_years],
        'OSF': [osf_data.get(y, 0) for y in all_years],
        'Figshare': [figshare_data.get(y, 0) for y in all_years]
    }

    source = ColumnDataSource(data=data)
    repositories = ["Zenodo", "OSF", "Figshare"]
    colors = ['#66c2a5', '#fc8d62', '#8da0cb']

    p = figure(
        x_range=data['year'],
        height=500,
        width=800,
        title="Number of datasets per year per data repository",
        tooltips=[
            ("Year", "@year"),
            ("Data repository", "$name"),
            ("Number of datasets", "@$name{0,0}")
        ],
        background_fill_color="#fafafa",
    )

    p.toolbar.active_drag = None

    p.vbar_stack(
        stackers=repositories,
        x='year',
        width=0.8,
        source=source,
        color=colors,
        legend_label=repositories
    )
    p.xaxis.axis_label = "Year"
    p.yaxis.axis_label = "Number of datasets"

    p.title.text_font_size = "14pt"
    p.xaxis.axis_label_text_font_size = "12pt"
    p.yaxis.axis_label_text_font_size = "12pt"
    p.xaxis.major_label_text_font_size = "10pt"
    p.yaxis.major_label_text_font_size = "10pt"

    p.legend.location = "top_left"
    p.legend.background_fill_alpha = 0.3
    p.legend.border_line_color = None
    p.legend.label_text_font_size = "10pt"

    return p
