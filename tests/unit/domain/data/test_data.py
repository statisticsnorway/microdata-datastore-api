# pylint: disable=protected-access
from unittest.mock import Mock

import pytest
from pyarrow import Table, dataset

from datastore_api.adapter.db.models import Datastore
from datastore_api.adapter.local_storage import datastore_directory
from datastore_api.common.exceptions import NotFoundException
from datastore_api.common.models import Version
from datastore_api.domain import data
from tests.resources import test_resources

ALL_COLUMNS = ["unit_id", "value", "start_epoch_days", "stop_epoch_days"]

stop_missing = ~dataset.field("stop_epoch_days").is_valid()
start_epoch_le_start = dataset.field("start_epoch_days") <= 3000
start_epoch_ge_start = dataset.field("start_epoch_days") >= 3000
start_epoch_le_stop = dataset.field("start_epoch_days") <= 10000
start_epoch_g_start = dataset.field("start_epoch_days") > 3000
stop_epoch_ge_start = dataset.field("stop_epoch_days") >= 3000
stop_epoch_le_stop = dataset.field("stop_epoch_days") <= 10000

FIND_BY_TIME_PERIOD_FILTER = (
    (start_epoch_le_start & stop_missing)
    | (start_epoch_le_start & stop_epoch_ge_start)
    | (start_epoch_ge_start & start_epoch_le_stop)
    | (start_epoch_g_start & stop_epoch_le_stop)
)
start_epoch_le_date = dataset.field("start_epoch_days") <= 17167
stop_epoch_ge_date = dataset.field("stop_epoch_days") >= 17167

FIND_BY_TIME_FILTER = (start_epoch_le_date & stop_missing) | (
    start_epoch_le_date & stop_epoch_ge_date
)

DATABASE_RESPONSE_OBJECT = Datastore(
    rdn="no.dev.test",
    description="Datastore for testing",
    directory="tests/resources/test_datastore",
    name="Test datastore",
    bump_enabled=True,
)


@pytest.fixture
def mock_db_client():
    mock = Mock()
    mock.get_datastore.return_value = DATABASE_RESPONSE_OBJECT
    return mock


@pytest.fixture
def patch_db(mock_db_client, monkeypatch):
    monkeypatch.setattr(
        datastore_directory.db, "get_database_client", lambda: mock_db_client
    )


def test_valid_event_request(patch_db):
    payload = test_resources.VALID_EVENT_QUERY_PERSON_INCOME_ALL
    file_name = data.process_event_request(
        payload.dataStructureName,
        payload.version,
        payload.population,
        payload.includeAttributes,
        payload.startDate,
        payload.stopDate,
    )
    assert parquet_table_to_csv_string(file_name) == (
        test_resources.PERSON_INCOME_ALL
    )


def test_valid_event_request_partitioned(patch_db):
    payload = test_resources.VALID_EVENT_QUERY_TEST_STUDIEPOENG_ALL
    file_name = data.process_event_request(
        payload.dataStructureName,
        payload.version,
        payload.population,
        payload.includeAttributes,
        payload.startDate,
        payload.stopDate,
    )
    assert parquet_table_to_csv_string(file_name) == (
        test_resources.TEST_STUDIEPOENG_ALL
    )


def test_event_request_causing_empty_result(patch_db):
    payload = test_resources.INVALID_EVENT_QUERY_INVALID_STOP_DATE
    result = data.process_event_request(
        payload.dataStructureName,
        payload.version,
        payload.population,
        payload.includeAttributes,
        payload.startDate,
        payload.stopDate,
    )
    assert isinstance(result, Table)
    assert result.num_columns == 2
    assert result.num_rows == 0


def test_valid_status_request(patch_db):
    payload = test_resources.VALID_STATUS_QUERY_PERSON_INCOME_LAST_ROW
    file_name = data.process_status_request(
        payload.dataStructureName,
        payload.version,
        payload.population,
        payload.includeAttributes,
        payload.date,
    )
    assert parquet_table_to_csv_string(file_name) == (
        test_resources.PERSON_INCOME_LAST_ROW
    )


def test_invalid_status_request(patch_db):
    payload = test_resources.INVALID_STATUS_QUERY_NOT_FOUND
    with pytest.raises(NotFoundException) as e:
        data.process_status_request(
            payload.dataStructureName,
            payload.version,
            payload.population,
            payload.includeAttributes,
            payload.date,
        )
    assert str(e.value) == (
        "No NOT_A_DATASET in data_versions file for version 1_0"
    )


def test_valid_fixed_request(patch_db):
    payload = test_resources.VALID_FIXED_QUERY_PERSON_INCOME_ALL
    file_name = data.process_fixed_request(
        payload.dataStructureName,
        payload.version,
        payload.population,
        payload.includeAttributes,
    )
    assert parquet_table_to_csv_string(file_name) == (
        test_resources.PERSON_INCOME_ALL
    )


def test_invalid_fixed_request(patch_db):
    payload = test_resources.INVALID_FIXED_QUERY_NOT_FOUND
    with pytest.raises(NotFoundException) as e:
        data.process_fixed_request(
            payload.dataStructureName,
            payload.version,
            payload.population,
            payload.includeAttributes,
        )
    assert str(e.value) == (
        "No NOT_A_DATASET in data_versions file for version 1_0"
    )


def parquet_table_to_csv_string(table):
    data_frame = table.to_pandas()
    csv_string = data_frame.to_csv(
        sep=";", encoding="utf-8", lineterminator="\n"
    )
    return csv_string


def test_read_parquet_no_filter(patch_db):
    expected_unit_ids = [
        11111111864482,
        11111112296273,
        11111113785911,
        11111113735577,
        11111111454434,
        11111111190644,
        11111113331169,
        11111111923572,
        11111112261125,
    ]
    expected_values = [
        "21529182",
        "12687840",
        "16354872",
        "12982099",
        "19330053",
        "11331198",
        "4166169",
        "7394257",
        "6926636",
    ]
    result = data._read_parquet(
        "TEST_PERSON_INCOME", Version.from_str("1.0.0.0"), None, ALL_COLUMNS
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    assert (
        len(result_dict["unit_id"])
        == len(result_dict["value"])
        == len(result_dict["start_epoch_days"])
        == len(result_dict["stop_epoch_days"])
    )
    result = data._read_parquet(
        "TEST_PERSON_INCOME", Version.from_str("1.0.0.0"), None, ALL_COLUMNS[:2]
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    assert len(result_dict.keys()) == 2


def test_read_parquet_fixed(patch_db):
    expected_unit_ids = [11111111864482, 11111112296273, 11111113785911]
    expected_values = ["21529182", "12687840", "16354872"]
    table_filter = dataset.field("unit_id").isin(expected_unit_ids)
    result = data._read_parquet(
        "TEST_PERSON_INCOME",
        Version.from_str("1.0.0.0"),
        table_filter,
        ALL_COLUMNS,
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    assert len(result_dict.keys()) == 4

    result = data._read_parquet(
        "TEST_PERSON_INCOME",
        Version.from_str("1.0.0.0"),
        table_filter,
        ALL_COLUMNS[:2],
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    assert len(result_dict.keys()) == 2


def test_read_parquet_time_period(patch_db):
    expected_unit_ids = [11111113735577, 11111111190644]
    expected_values = ["12982099", "11331198"]
    result = data._read_parquet(
        "TEST_PERSON_INCOME",
        Version.from_str("1.0.0.0"),
        FIND_BY_TIME_PERIOD_FILTER,
        ALL_COLUMNS,
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    epoch_days = (
        result_dict["start_epoch_days"] + result_dict["stop_epoch_days"]
    )
    for epoch_day in epoch_days:
        assert epoch_day <= 10000 and epoch_day >= 3000


def test_read_parquet_time_period_with_pop_filter(patch_db):
    expected_unit_ids = [11111113735577]
    expected_values = ["12982099"]
    table_filter = FIND_BY_TIME_PERIOD_FILTER & dataset.field("unit_id").isin(
        expected_unit_ids
    )
    result = data._read_parquet(
        "TEST_PERSON_INCOME",
        Version.from_str("1.0.0.0"),
        table_filter,
        ALL_COLUMNS,
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    epoch_days = (
        result_dict["start_epoch_days"] + result_dict["stop_epoch_days"]
    )
    for epoch_day in epoch_days:
        assert epoch_day <= 10000 and epoch_day >= 3000


def test_read_parquet_time(patch_db):
    expected_unit_ids = [11111111864482, 11111112296273]
    expected_values = ["21529182", "12687840"]
    result = data._read_parquet(
        "TEST_PERSON_INCOME",
        Version.from_str("1.0.0.0"),
        FIND_BY_TIME_FILTER,
        ALL_COLUMNS,
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    for epoch_day in result_dict["start_epoch_days"]:
        assert epoch_day <= 17167
    for epoch_day in result_dict["stop_epoch_days"]:
        assert epoch_day >= 17167


def test_read_parquet_time_with_pop_filter(patch_db):
    expected_unit_ids = [11111111864482]
    expected_values = ["21529182"]
    table_filter = FIND_BY_TIME_FILTER & dataset.field("unit_id").isin(
        expected_unit_ids
    )
    result = data._read_parquet(
        "TEST_PERSON_INCOME",
        Version.from_str("1.0.0.0"),
        table_filter,
        ALL_COLUMNS,
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    for epoch_day in result_dict["start_epoch_days"]:
        assert epoch_day <= 17167
    for epoch_day in result_dict["stop_epoch_days"]:
        assert epoch_day >= 17167
