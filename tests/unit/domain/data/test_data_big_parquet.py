import pytest
import os

import pyarrow
import numpy
from pyarrow import dataset, parquet

from datastore_api.common.models import Version
from datastore_api.domain import data

RESOURCE_DIR = "tests/resources/results"
DATASET_NAME = "TEST_BIG"
ALL_COLUMNS = ["unit_id", "value", "start_epoch_days", "stop_epoch_days"]


def setup_function():
    dataset_path = os.path.join(RESOURCE_DIR, DATASET_NAME)
    os.makedirs(dataset_path, exist_ok=True)

    num_rows = 2_000_000
    unit_id_array = pyarrow.array(
        numpy.arange(1, num_rows + 1), type=pyarrow.int32()
    )
    value_array = pyarrow.array(
        numpy.random.choice(["01", "02"], size=num_rows), type=pyarrow.string()
    )
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


def teardown_function():
    os.remove(f"{RESOURCE_DIR}/{DATASET_NAME}/{DATASET_NAME}__1_0.parquet")


@pytest.mark.skipif("not config.getoption('include-big-data')")
def test_read_big_parquet_with_big_pop_filter():
    expected_unit_ids = [i for i in range(1, 1_000_001)]
    table_filter = dataset.field("unit_id").isin(expected_unit_ids)
    result = data._read_parquet(
        DATASET_NAME, Version.from_str("1.0.0.0"), table_filter, ALL_COLUMNS
    )
    result_dict = result.to_pydict()
    assert len(result_dict["unit_id"]) == 1_000_000
