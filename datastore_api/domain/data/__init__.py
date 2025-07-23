import logging
from typing import Union

from pyarrow import Table
from pyarrow import dataset

from datastore_api.domain.data import filters
from datastore_api.adapter.local_storage import data_directory
from datastore_api.api.data.models import (
    InputTimePeriodQuery,
    InputTimeQuery,
    InputFixedQuery,
)

logger = logging.getLogger()

EMPTY_RESULT_TEXT = "empty_result"
ALL_COLUMNS = ["unit_id", "value", "start_epoch_days", "stop_epoch_days"]


def process_event_request(
    input_query: InputTimePeriodQuery,
) -> Union[Table, str]:
    table_filter = filters.generate_time_period_filter(
        input_query.startDate, input_query.stopDate, input_query.population
    )
    columns = ALL_COLUMNS if input_query.includeAttributes else ALL_COLUMNS[:2]
    return _read_parquet(
        input_query.dataStructureName,
        input_query.get_file_version(),
        table_filter,
        columns,
    )


def process_status_request(input_query: InputTimeQuery) -> Union[Table, str]:
    table_filter = filters.generate_time_filter(
        input_query.date, input_query.population
    )
    columns = ALL_COLUMNS if input_query.includeAttributes else ALL_COLUMNS[:2]
    return _read_parquet(
        input_query.dataStructureName,
        input_query.get_file_version(),
        table_filter,
        columns,
    )


def process_fixed_request(input_query: InputFixedQuery) -> Union[Table, str]:
    table_filter = filters.generate_population_filter(input_query.population)
    columns = ALL_COLUMNS if input_query.includeAttributes else ALL_COLUMNS[:2]
    return _read_parquet(
        input_query.dataStructureName,
        input_query.get_file_version(),
        table_filter,
        columns,
    )


def _read_parquet(
    dataset_name: str,
    version: str,
    table_filter: dataset.Expression,
    columns: list[str],
) -> Table:
    """
    Reads and filters a parquet file or partition and returns a
    pyarrow.Table with the requested columns.

    * dataset_name: str - name of dataset
    * version: str - '<MAJOR>_<MINOR>' formatted semantic version
    * table_filter: dataset.Expression - filters applied to the table
    * columns: list[str] - names of the columns to include in the
                           returned table
    """
    parquet_path = data_directory.get_parquet_file_path(dataset_name, version)
    table = dataset.dataset(parquet_path).to_table(
        filter=table_filter, columns=columns
    )
    logger.info(f"Number of rows in result set: {table.num_rows}")
    return table
