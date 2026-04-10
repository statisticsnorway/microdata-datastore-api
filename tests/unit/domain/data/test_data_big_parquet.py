import os
import shutil
from pathlib import Path

import numpy
import pyarrow
import pytest
from pyarrow import parquet

from datastore_api.common.models import Version
from datastore_api.domain.data import process_fixed_request

DATASTORE_DIR = Path("tests/resources/test_datastore")
DATASET_NAME = "TEST_BIG"
DATASET_DIR = os.path.join(DATASTORE_DIR, "data", DATASET_NAME)
ALL_COLUMNS = ["unit_id", "value", "start_epoch_days", "stop_epoch_days"]


@pytest.fixture(scope="module")
def big_parquet_dataset():
    _create_big_parquet()
    yield
    shutil.rmtree(DATASET_DIR)


def _create_big_parquet():
    dataset_path = os.path.join(DATASTORE_DIR, "data", DATASET_NAME)
    os.makedirs(dataset_path, exist_ok=True)

    num_rows = 2_000_000
    unit_id_array = pyarrow.array(
        numpy.arange(1, num_rows + 1), type=pyarrow.int32()
    )

    value_counts = {
        "001": 200_000,
        "002": 400_000,
        "003": 600_000,
        "030": 250_000,
        "031": 500_000,
        "032": 50_000,
    }
    values = []
    for value, count in value_counts.items():
        values.extend([value] * count)

    value_array = pyarrow.array(values, type=pyarrow.string())
    start_epoch_days_array = pyarrow.nulls(num_rows, type=pyarrow.int32())
    stop_epoch_days_array = pyarrow.array(
        [18123] * num_rows, type=pyarrow.int32()
    )

    table = pyarrow.table(
        {
            "unit_id": unit_id_array,
            "value": value_array,
            "start_epoch_days": start_epoch_days_array,
            "stop_epoch_days": stop_epoch_days_array,
        }
    )
    parquet.write_table(
        table, os.path.join(dataset_path, f"{DATASET_NAME}__1_0.parquet")
    )


@pytest.mark.parametrize(
    "population_filter, value_filter, expected_count",
    [
        (list(range(1, 1_000_001)), None, 1_000_000),
        (list(range(1, 1_000_001)), ["001"], 200_000),
        (list(range(1, 1_000_001)), ["003"], 400_000),
        (None, ["03*"], 800_000),
        (None, ["0*"], 2_000_000),
        (None, None, 2_000_000),
    ],
)
@pytest.mark.skipif("not config.getoption('include-big-data')")
def test_read_big_parquet_with_big_pop_and_value_filter(
    big_parquet_dataset, population_filter, value_filter, expected_count
):
    result = process_fixed_request(
        dataset_name=DATASET_NAME,
        version=Version.from_str("1.0.0.0"),
        population=population_filter,
        include_attributes=True,
        values=value_filter,
        datastore_root_dir=DATASTORE_DIR,
    )
    result_dict = result.to_pydict()
    assert len(result_dict["unit_id"]) == expected_count
