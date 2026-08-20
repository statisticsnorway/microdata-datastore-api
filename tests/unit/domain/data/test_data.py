# pylint: disable=protected-access

import json
import os
import shutil
from pathlib import Path

import pyarrow
import pytest
from pyarrow import Table, dataset, parquet

from datastore_api.adapter.db.models import Datastore
from datastore_api.common.exceptions import NotFoundException
from datastore_api.common.models import Version
from datastore_api.domain.data import (
    EncryptedDataReader,
    UnencryptedDataReader,
    _get_parquet_path,
    generate_data_filter,
    select_data_reader,
)
from datastore_api.domain.data.models import (
    InputFixedQuery,
)
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

DATASTORE = Datastore(
    datastore_id=1,
    rdn="no.dev.test",
    description="Datastore for testing",
    directory="tests/resources/test_datastore",
    name="Test datastore",
    bump_enabled=True,
)

DATASTORE_ROOT_DIR = Path("tests/resources/test_datastore")


@pytest.fixture
def fixed_dataset_parquet(scope="module"):
    table = pyarrow.table(
        {
            "unit_id": [1, 2, 3, 4, 5, 6, 7],
            "value": ["0012", "0013", "0015", "0020", "0025", "0100", "2100"],
            "start_epoch_days": [None] * 7,
            "stop_epoch_days": [20553] * 7,
        }
    )
    dataset_path = os.path.join(
        DATASTORE_ROOT_DIR, "data", "TEST_FIXED_DATASET"
    )
    os.makedirs(dataset_path, exist_ok=True)

    parquet.write_table(
        table, os.path.join(dataset_path, "TEST_FIXED_DATASET__1_0.parquet")
    )
    yield
    shutil.rmtree(dataset_path)


def test_valid_event_request():
    payload = test_resources.VALID_EVENT_QUERY_PERSON_INCOME_ALL
    data_filter = generate_data_filter(payload)
    columns = ALL_COLUMNS if payload.includeAttributes else ALL_COLUMNS[:2]
    file_name = UnencryptedDataReader(
        parquet_path=_get_parquet_path(
            payload.version, payload.dataStructureName, DATASTORE_ROOT_DIR
        ),
        columns=columns,
    ).read_data(
        data_filter,
    )
    assert parquet_table_to_csv_string(file_name) == (
        test_resources.PERSON_INCOME_ALL
    )


def test_valid_event_request_partitioned():
    payload = test_resources.VALID_EVENT_QUERY_TEST_STUDIEPOENG_ALL
    data_filter = generate_data_filter(payload)
    columns = ALL_COLUMNS if payload.includeAttributes else ALL_COLUMNS[:2]
    file_name = UnencryptedDataReader(
        parquet_path=_get_parquet_path(
            payload.version, payload.dataStructureName, DATASTORE_ROOT_DIR
        ),
        columns=columns,
    ).read_data(
        data_filter,
    )
    assert parquet_table_to_csv_string(file_name) == (
        test_resources.TEST_STUDIEPOENG_ALL
    )


def test_event_request_causing_empty_result():
    payload = test_resources.INVALID_EVENT_QUERY_INVALID_STOP_DATE
    data_filter = generate_data_filter(payload)
    columns = ALL_COLUMNS if payload.includeAttributes else ALL_COLUMNS[:2]
    result = UnencryptedDataReader(
        parquet_path=_get_parquet_path(
            payload.version, payload.dataStructureName, DATASTORE_ROOT_DIR
        ),
        columns=columns,
    ).read_data(
        data_filter,
    )
    assert isinstance(result, Table)
    assert result.num_columns == 2
    assert result.num_rows == 0


def test_valid_status_request():
    payload = test_resources.VALID_STATUS_QUERY_PERSON_INCOME_LAST_ROW
    data_filter = generate_data_filter(payload)
    columns = ALL_COLUMNS if payload.includeAttributes else ALL_COLUMNS[:2]
    file_name = UnencryptedDataReader(
        parquet_path=_get_parquet_path(
            payload.version, payload.dataStructureName, DATASTORE_ROOT_DIR
        ),
        columns=columns,
    ).read_data(
        data_filter,
    )
    assert parquet_table_to_csv_string(file_name) == (
        test_resources.PERSON_INCOME_LAST_ROW
    )


def test_invalid_status_request():
    payload = test_resources.INVALID_STATUS_QUERY_NOT_FOUND
    data_filter = generate_data_filter(payload)
    columns = ALL_COLUMNS if payload.includeAttributes else ALL_COLUMNS[:2]
    with pytest.raises(NotFoundException) as e:
        UnencryptedDataReader(
            parquet_path=_get_parquet_path(
                payload.version, payload.dataStructureName, DATASTORE_ROOT_DIR
            ),
            columns=columns,
        ).read_data(
            data_filter,
        )
    assert str(e.value) == (
        "No NOT_A_DATASET in data_versions file for version 1_0"
    )


def test_valid_fixed_request():
    payload = test_resources.VALID_FIXED_QUERY_PERSON_INCOME_ALL
    data_filter = generate_data_filter(payload)
    columns = ALL_COLUMNS if payload.includeAttributes else ALL_COLUMNS[:2]
    file_name = UnencryptedDataReader(
        parquet_path=_get_parquet_path(
            payload.version, payload.dataStructureName, DATASTORE_ROOT_DIR
        ),
        columns=columns,
    ).read_data(
        data_filter,
    )
    assert parquet_table_to_csv_string(file_name) == (
        test_resources.PERSON_INCOME_ALL
    )


def test_invalid_fixed_request():
    payload = test_resources.INVALID_FIXED_QUERY_NOT_FOUND
    data_filter = generate_data_filter(payload)
    columns = ALL_COLUMNS if payload.includeAttributes else ALL_COLUMNS[:2]
    with pytest.raises(NotFoundException) as e:
        UnencryptedDataReader(
            parquet_path=_get_parquet_path(
                payload.version, payload.dataStructureName, DATASTORE_ROOT_DIR
            ),
            columns=columns,
        ).read_data(
            data_filter,
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


def test_read_parquet_no_filter():
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
    result = UnencryptedDataReader(
        parquet_path=_get_parquet_path(
            Version.from_str("1.0.0.0"),
            "TEST_PERSON_INCOME",
            DATASTORE_ROOT_DIR,
        ),
        columns=ALL_COLUMNS,
    ).read_data(
        None,
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
    result = UnencryptedDataReader(
        parquet_path=_get_parquet_path(
            Version.from_str("1.0.0.0"),
            "TEST_PERSON_INCOME",
            DATASTORE_ROOT_DIR,
        ),
        columns=ALL_COLUMNS[:2],
    ).read_data(
        None,
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    assert len(result_dict.keys()) == 2


def test_read_parquet_fixed():
    expected_unit_ids = [11111111864482, 11111112296273, 11111113785911]
    expected_values = ["21529182", "12687840", "16354872"]
    table_filter = dataset.field("unit_id").isin(expected_unit_ids)
    result = UnencryptedDataReader(
        parquet_path=_get_parquet_path(
            Version.from_str("1.0.0.0"),
            "TEST_PERSON_INCOME",
            DATASTORE_ROOT_DIR,
        ),
        columns=ALL_COLUMNS,
    ).read_data(
        table_filter,
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    assert len(result_dict.keys()) == 4

    result = UnencryptedDataReader(
        parquet_path=_get_parquet_path(
            Version.from_str("1.0.0.0"),
            "TEST_PERSON_INCOME",
            DATASTORE_ROOT_DIR,
        ),
        columns=ALL_COLUMNS[:2],
    ).read_data(
        table_filter,
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    assert len(result_dict.keys()) == 2


def test_read_parquet_time_period():
    expected_unit_ids = [11111113735577, 11111111190644]
    expected_values = ["12982099", "11331198"]
    result = UnencryptedDataReader(
        parquet_path=_get_parquet_path(
            Version.from_str("1.0.0.0"),
            "TEST_PERSON_INCOME",
            DATASTORE_ROOT_DIR,
        ),
        columns=ALL_COLUMNS,
    ).read_data(
        FIND_BY_TIME_PERIOD_FILTER,
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    epoch_days = (
        result_dict["start_epoch_days"] + result_dict["stop_epoch_days"]
    )
    for epoch_day in epoch_days:
        assert epoch_day <= 10000 and epoch_day >= 3000


def test_read_parquet_time_period_with_pop_filter():
    expected_unit_ids = [11111113735577]
    expected_values = ["12982099"]
    table_filter = FIND_BY_TIME_PERIOD_FILTER & dataset.field("unit_id").isin(
        expected_unit_ids
    )
    result = UnencryptedDataReader(
        parquet_path=_get_parquet_path(
            Version.from_str("1.0.0.0"),
            "TEST_PERSON_INCOME",
            DATASTORE_ROOT_DIR,
        ),
        columns=ALL_COLUMNS,
    ).read_data(
        table_filter,
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    epoch_days = (
        result_dict["start_epoch_days"] + result_dict["stop_epoch_days"]
    )
    for epoch_day in epoch_days:
        assert epoch_day <= 10000 and epoch_day >= 3000


def test_read_parquet_time():
    expected_unit_ids = [11111111864482, 11111112296273]
    expected_values = ["21529182", "12687840"]
    result = UnencryptedDataReader(
        parquet_path=_get_parquet_path(
            Version.from_str("1.0.0.0"),
            "TEST_PERSON_INCOME",
            DATASTORE_ROOT_DIR,
        ),
        columns=ALL_COLUMNS,
    ).read_data(
        FIND_BY_TIME_FILTER,
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    for epoch_day in result_dict["start_epoch_days"]:
        assert epoch_day <= 17167
    for epoch_day in result_dict["stop_epoch_days"]:
        assert epoch_day >= 17167


def test_read_parquet_time_with_pop_filter():
    expected_unit_ids = [11111111864482]
    expected_values = ["21529182"]
    table_filter = FIND_BY_TIME_FILTER & dataset.field("unit_id").isin(
        expected_unit_ids
    )
    result = UnencryptedDataReader(
        parquet_path=_get_parquet_path(
            Version.from_str("1.0.0.0"),
            "TEST_PERSON_INCOME",
            DATASTORE_ROOT_DIR,
        ),
        columns=ALL_COLUMNS,
    ).read_data(
        table_filter,
    )
    result_dict = result.to_pydict()
    assert result_dict["unit_id"] == expected_unit_ids
    assert result_dict["value"] == expected_values
    for epoch_day in result_dict["start_epoch_days"]:
        assert epoch_day <= 17167
    for epoch_day in result_dict["stop_epoch_days"]:
        assert epoch_day >= 17167


def test_read_parquet_with_exact_string_value_filter(fixed_dataset_parquet):
    expected_values = ["0012", "0100"]
    data_filter = generate_data_filter(
        InputFixedQuery(
            values=expected_values,
            dataStructureName="TEST_FIXED_DATASET",
            version=Version.from_str("1.0.0.0"),  # NOSONAR
        )
    )
    result_dict = (
        UnencryptedDataReader(
            parquet_path=_get_parquet_path(
                Version.from_str("1.0.0.0"),
                "TEST_FIXED_DATASET",
                DATASTORE_ROOT_DIR,
            ),
            columns=ALL_COLUMNS,
        )
        .read_data(
            data_filter,
        )
        .to_pydict()
    )
    assert result_dict["value"] == expected_values


def test_read_parquet_with_wildcard_value_filter(fixed_dataset_parquet):
    expected_values = ["0020", "0025", "2100"]
    data_filter = generate_data_filter(
        InputFixedQuery(
            values=["002*", "2*"],
            dataStructureName="TEST_FIXED_DATASET",
            version=Version.from_str("1.0.0.0"),  # NOSONAR
        )
    )
    result_dict = (
        UnencryptedDataReader(
            parquet_path=_get_parquet_path(
                Version.from_str("1.0.0.0"),
                "TEST_FIXED_DATASET",
                DATASTORE_ROOT_DIR,
            ),
            columns=ALL_COLUMNS,
        )
        .read_data(
            data_filter,
        )
        .to_pydict()
    )
    assert sorted(result_dict["value"]) == expected_values


def test_read_parquet_with_combined_population_and_value_filter(
    fixed_dataset_parquet,
):
    expected_values = ["0012", "0015"]
    payload = InputFixedQuery(
        dataStructureName="TEST_FIXED_DATASET",
        version=Version.from_str("1.0.0.0"),  # NOSONAR
        population=[1, 3],
        includeAttributes=True,
        values=["001*"],
    )
    data_filter = generate_data_filter(payload)
    columns = ALL_COLUMNS if payload.includeAttributes else ALL_COLUMNS[:2]
    result_dict = (
        UnencryptedDataReader(
            parquet_path=_get_parquet_path(
                payload.version, payload.dataStructureName, DATASTORE_ROOT_DIR
            ),
            columns=columns,
        )
        .read_data(
            data_filter,
        )
        .to_pydict()
    )
    assert result_dict["value"] == expected_values


def test_select_data_reader_returns_encrypted_reader(tmp_path):
    datastore_dir = tmp_path / "datastore"
    datastore_dir.mkdir()
    (datastore_dir / "encrypted_versions.json").write_text(
        json.dumps({"versions": ["1.0"]})
    )
    (datastore_dir / "data_versions__1_0.json").write_text(
        json.dumps({"TEST_FIXED_DATASET": "TEST_FIXED_DATASET__1_0.parquet"})
    )
    dataset_dir = tmp_path / "data" / "TEST_FIXED_DATASET"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "TEST_FIXED_DATASET__1_0.parquet").touch()
    query = InputFixedQuery(
        dataStructureName="TEST_FIXED_DATASET",
        version=Version.from_str("1.0.0.0"),  # NOSONAR
    )
    reader = select_data_reader(query, tmp_path)
    assert isinstance(reader, EncryptedDataReader)


def test_select_data_reader_returns_unencrypted_reader(
    tmp_path,
):
    datastore_dir = tmp_path / "datastore"
    datastore_dir.mkdir()
    (datastore_dir / "encrypted_versions.json").write_text(
        json.dumps({"versions": ["2_0"]})
    )
    (datastore_dir / "data_versions__1_0.json").write_text(
        json.dumps({"TEST_FIXED_DATASET": "TEST_FIXED_DATASET__1_0.parquet"})
    )
    dataset_dir = tmp_path / "data" / "TEST_FIXED_DATASET"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "TEST_FIXED_DATASET__1_0.parquet").touch()
    query = InputFixedQuery(
        dataStructureName="TEST_FIXED_DATASET",
        version=Version.from_str("1.0.0.0"),  # NOSONAR
    )
    reader = select_data_reader(query, tmp_path)
    assert isinstance(reader, UnencryptedDataReader)


def test_select_data_reader_returns_unencr_reader_when_no_encr_versions_file(
    tmp_path,
):
    datastore_dir = tmp_path / "datastore"
    datastore_dir.mkdir()
    (datastore_dir / "data_versions__1_0.json").write_text(
        json.dumps({"TEST_FIXED_DATASET": "TEST_FIXED_DATASET__1_0.parquet"})
    )
    dataset_dir = tmp_path / "data" / "TEST_FIXED_DATASET"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "TEST_FIXED_DATASET__1_0.parquet").touch()
    query = InputFixedQuery(
        dataStructureName="TEST_FIXED_DATASET",
        version=Version.from_str("1.0.0.0"),  # NOSONAR
    )
    reader = select_data_reader(query, tmp_path)
    assert isinstance(reader, UnencryptedDataReader)


def test_select_data_reader_uses_actual_data_version_not_requested_version(
    tmp_path,
):
    """
    A dataset requested at version 50.0 may not have changed since
    version 49.0. In that case the encryption status of version 49.0
    (the version the data was actually published in) should be used,
    not the encryption status of the requested version 50.0.
    """
    datastore_dir = tmp_path / "datastore"
    datastore_dir.mkdir()
    (datastore_dir / "encrypted_versions.json").write_text(
        json.dumps({"versions": ["49.0"]})
    )
    (datastore_dir / "data_versions__50_0.json").write_text(
        json.dumps({"TEST_FIXED_DATASET": "TEST_FIXED_DATASET__49_0.parquet"})
    )
    dataset_dir = tmp_path / "data" / "TEST_FIXED_DATASET"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "TEST_FIXED_DATASET__49_0.parquet").touch()
    query = InputFixedQuery(
        dataStructureName="TEST_FIXED_DATASET",
        version=Version.from_str("50.0.0.0"),  # NOSONAR
    )
    reader = select_data_reader(query, tmp_path)
    assert isinstance(reader, EncryptedDataReader)


def test_select_data_reader_uses_actual_data_version_when_unencrypted(
    tmp_path,
):
    """
    Conversely, requesting version 50.0 where the data was actually last
    published unencrypted in version 49.0 should return an unencrypted
    reader, even if version 50.0 itself is encrypted.
    """
    datastore_dir = tmp_path / "datastore"
    datastore_dir.mkdir()
    (datastore_dir / "encrypted_versions.json").write_text(
        json.dumps({"versions": ["50.0"]})
    )
    (datastore_dir / "data_versions__50_0.json").write_text(
        json.dumps({"TEST_FIXED_DATASET": "TEST_FIXED_DATASET__49_0.parquet"})
    )
    dataset_dir = tmp_path / "data" / "TEST_FIXED_DATASET"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "TEST_FIXED_DATASET__49_0.parquet").touch()
    query = InputFixedQuery(
        dataStructureName="TEST_FIXED_DATASET",
        version=Version.from_str("50.0.0.0"),  # NOSONAR
    )
    reader = select_data_reader(query, tmp_path)
    assert isinstance(reader, UnencryptedDataReader)
