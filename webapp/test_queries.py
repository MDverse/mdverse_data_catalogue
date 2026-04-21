from loguru import logger

from pathlib import Path
import time
from datetime import timedelta
from functools import partial

from sqlmodel import Session
from db_schema import engine
from queries import(
    # Queries for index.html
    get_dataset_origin_summary,
    get_titles,
    generate_title_wordcloud,
    get_files_yearly_counts_for_origin,
    create_files_plot,
    get_dataset_yearly_counts_for_origin,
    create_datasets_plot,
    # Queries for file_types.html
    get_file_type_stats,
    get_tsv_depending_on_type,
    # Queries for search.html
    get_all_datasets,
    get_dataset_info_by_id,
    # Queries for dataset_file_info.html
    get_all_files_from_dataset,
    get_param_files,
    get_top_files,
    get_traj_files,
)

# Log file format
# Log file will be erased at each run
# Remove mode="w" to keep log file between runs
logger.add(
    f"{Path(__file__).stem}.log",
    mode="w",
    format="{time:YYYY-MM-DDTHH:mm:ss} | <lvl>{level:<8} | {message}</lvl>",
    level="DEBUG",
)

# def time_query(query_function):
#     start = time.perf_counter()
#     query_function()
#     end = time.perf_counter()
#     logger.info(f"Time taken for {query_function.__name__:^40}: {timedelta(seconds=end - start)}\n")


def time_query(query_function):
    start = time.perf_counter()
    query_function()
    end = time.perf_counter()
    elapsed_seconds = end - start
    # Format elapsed_seconds to have exactly two digits before the decimal point
    # and six digits after, zero padded.
    formatted_time = f"{elapsed_seconds:09.6f}"
    logger.info(f"Time taken for {query_function.__name__:^40}: {formatted_time} seconds\n")



def test_all_queries_and_time_them():
    # Queries for index.html
    time_query(get_dataset_origin_summary)
    time_query(get_titles)
    time_query(generate_title_wordcloud)

    with Session(engine) as session:
        # Bind the parameters using partial
        partial_get_files = partial(get_files_yearly_counts_for_origin, session, "zenodo")
        # Manually set the __name__
        partial_get_files.__name__ = "get_files_yearly_counts_for_origin"
        time_query(partial_get_files)

        partial_get_datasets = partial(get_dataset_yearly_counts_for_origin, session, "zenodo")
        partial_get_datasets.__name__ = "get_dataset_yearly_counts_for_origin"
        time_query(partial_get_datasets)

    time_query(create_files_plot)
    time_query(create_datasets_plot)

    # Queries for file_types.html
    time_query(get_file_type_stats)

    partial_get_tsv = partial(get_tsv_depending_on_type, "xtc")
    partial_get_tsv.__name__ = "get_tsv_depending_on_type"
    time_query(partial_get_tsv)

    # Queries for search.html
    time_query(get_all_datasets)

    partial_get_dataset_info_by_id = partial(get_dataset_info_by_id, 6)
    partial_get_dataset_info_by_id.__name__ = "get_dataset_info_by_id"
    time_query(partial_get_dataset_info_by_id)

    # Queries for dataset_file_info.html
    partial_get_files_from_dataset = partial(get_all_files_from_dataset, 6)
    partial_get_files_from_dataset.__name__ = "get_all_files_from_dataset"
    time_query(partial_get_files_from_dataset)

    time_query(get_param_files)
    time_query(get_top_files)
    time_query(get_traj_files)



if __name__ == "__main__":
    test_all_queries_and_time_them()

