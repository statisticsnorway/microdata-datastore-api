import logging
from typing import Union

from pyarrow import Table
from pyarrow import dataset

from datastore_api.common.models import Version
from datastore_api.domain.data import filters
from datastore_api.adapter.local_storage import (
    datastore_directory,
)


logger = logging.getLogger()

EMPTY_RESULT_TEXT = "empty_result"
ALL_COLUMNS = ["unit_id", "value", "start_epoch_days", "stop_epoch_days"]


def process_event_request(
    dataset_name: str,
    version: Version,
    population: list | None,
    include_attributes: bool,
    start_date: int,
    stop_date: int,
) -> Table:
    table_filter = filters.generate_time_period_filter(
        start_date, stop_date, population
    )
    columns = ALL_COLUMNS if include_attributes else ALL_COLUMNS[:2]
    return _read_parquet(
        dataset_name,
        version,
        table_filter,
        columns,
    )


def process_status_request(
    dataset_name: str,
    version: Version,
    population: list | None,
    include_attributes: bool,
    date: int,
) -> Table:
    table_filter = filters.generate_time_filter(date, population)
    columns = ALL_COLUMNS if include_attributes else ALL_COLUMNS[:2]
    return _read_parquet(
        dataset_name,
        version,
        table_filter,
        columns,
    )


def process_fixed_request(
    dataset_name: str,
    version: Version,
    population: list | None,
    include_attributes: bool,
) -> Table:
    table_filter = filters.generate_population_filter(population)
    columns = ALL_COLUMNS if include_attributes else ALL_COLUMNS[:2]
    return _read_parquet(
        dataset_name,
        version,
        table_filter,
        columns,
    )


def _read_parquet(
    dataset_name: str,
    version: Version,
    table_filter: dataset.Expression,
    columns: list[str],
) -> Table:
    """
    Reads and filters a parquet file or partition and returns a
    pyarrow.Table with the requested columns.
    If a draft version is requested, but no draft updated data exists
    for the given dataset, it will fall back to the latest released
    version of that dataset.

    * dataset_name: str - name of dataset
    * version: Version - formatted semantic version
    * table_filter: dataset.Expression - filters applied to the table
    * columns: list[str] - names of the columns to include in the
                           returned table
    """
    parquet_path: str | None = None
    if version.is_draft():
        parquet_path = datastore_directory.get_draft_data_file_path(
            dataset_name
        )
    else:
        parquet_path = datastore_directory.get_data_path_from_data_versions(
            dataset_name, version
        )

    if parquet_path is None:
        latest_version = datastore_directory.get_latest_version()
        datastore_directory.get_data_path_from_data_versions(
            dataset_name, latest_version
        )
    table = dataset.dataset(parquet_path).to_table(
        filter=table_filter, columns=columns
    )
    logger.info(f"Number of rows in result set: {table.num_rows}")
    return table
