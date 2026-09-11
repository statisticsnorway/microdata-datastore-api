from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from datastore_api.adapter import db
from datastore_api.adapter.auth.dependencies import authorize_user
from datastore_api.api.common.dependencies import (
    get_data_reader,
)
from datastore_api.main import app

FAKE_RESULT_FILE_NAME = "fake_result_file_name"
MOCK_RESULT = pq.read_table("tests/resources/results/mocked_result.parquet")


@pytest.fixture
def mock_data_reader():
    return Mock(read_data=Mock(return_value=MOCK_RESULT))


@pytest.fixture
def mock_db_client():
    mock = Mock()
    mock.get_datastore.return_value = SimpleNamespace(
        directory=str("tests/resources/test_datastore")
    )
    return mock


@pytest.fixture
def mock_auth_deps():
    return {
        "user": Mock(return_value=None),
    }


@pytest.fixture
def client(mock_db_client: Mock, mock_auth_deps: dict, mock_data_reader: Mock):
    app.dependency_overrides[db.get_database_client] = lambda: mock_db_client
    app.dependency_overrides[authorize_user] = lambda: mock_auth_deps["user"]()
    app.dependency_overrides[get_data_reader] = lambda: mock_data_reader
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_data_event_stream_result(client: TestClient, mock_auth_deps: dict):
    response = client.post(
        "/datastores/no.ssb.test/data/event/stream",
        json={
            "version": "1.0.0.0",
            "dataStructureName": "FAKE_NAME",
            "startDate": 0,
            "stopDate": 0,
        },
        headers={"Authorization": "Bearer valid-token"},
    )
    mock_auth_deps["user"].assert_called_once()
    reader = pa.BufferReader(response.content)
    assert response.status_code == 200
    assert pq.read_table(reader) == MOCK_RESULT


def test_data_status_stream_result(client: TestClient, mock_auth_deps: dict):
    response = client.post(
        "/datastores/no.ssb.test/data/status/stream",
        json={
            "version": "1.0.0.0",
            "dataStructureName": "FAKE_NAME",
            "date": 0,
        },
        headers={"Authorization": "Bearer valid-token"},
    )
    mock_auth_deps["user"].assert_called_once()
    reader = pa.BufferReader(response.content)
    assert response.status_code == 200
    assert pq.read_table(reader) == MOCK_RESULT


def test_data_fixed_stream_result(client: TestClient, mock_auth_deps: dict):
    response = client.post(
        "/datastores/no.ssb.test/data/fixed/stream",
        json={"version": "1.0.0.0", "dataStructureName": "FAKE_NAME"},
        headers={"Authorization": "Bearer valid-token"},
    )
    mock_auth_deps["user"].assert_called_once()
    reader = pa.BufferReader(response.content)
    assert response.status_code == 200
    assert pq.read_table(reader) == MOCK_RESULT


@pytest.mark.parametrize(
    "temporality, expected_ids, filtered_ids",
    [
        ("fixed", [1, 2, 3, 4, 5], [1, 3, 4]),
        ("status", [3, 5], [3]),
        ("event", [1, 2, 3, 5], [1, 3]),
    ],
)
@pytest.mark.parametrize("with_filters", [False, True])
def test_data_stream_uses_endpoint_filter(
    client,
    mock_data_reader,
    temporality,
    expected_ids,
    filtered_ids,
    with_filters,
):
    table = pa.table(
        {
            "unit_id": [1, 2, 3, 4, 5],
            "value": ["A", "A", "A", "A", "B"],
            "start_epoch_days": [0, 5, 10, 20, 0],
            "stop_epoch_days": [4, 9, None, None, None],
        }
    )
    # Extra date fields must not change which filter the endpoint uses.
    payload = {
        "version": "1.0.0.0",
        "dataStructureName": "FAKE_NAME",
        "date": 10,
        "startDate": 4,
        "stopDate": 10,
    }
    if with_filters:
        payload.update(population=[1, 3, 4, 5], values=["A"])

    response = client.post(
        f"/datastores/no.ssb.test/data/{temporality}/stream",
        json=payload,
    )

    assert response.status_code == 200
    mock_data_reader.read_data.assert_called_once()
    data_filter = mock_data_reader.read_data.call_args.args[0]
    if temporality == "fixed" and not with_filters:
        assert data_filter is None
    else:
        table = table.filter(data_filter)
    assert table["unit_id"].to_pylist() == (
        filtered_ids if with_filters else expected_ids
    )


@pytest.mark.parametrize(
    "temporality, dates, missing_field",
    [
        ("status", {}, "date"),
        ("event", {"stopDate": 10}, "startDate"),
        ("event", {"startDate": 4}, "stopDate"),
    ],
)
def test_data_stream_requires_dates(
    client, mock_data_reader, temporality, dates, missing_field
):
    response = client.post(
        f"/datastores/no.ssb.test/data/{temporality}/stream",
        json={
            "version": "1.0.0.0",
            "dataStructureName": "FAKE_NAME",
            **dates,
        },
    )

    assert response.status_code == 400
    assert any(
        error["loc"] == ["body", missing_field]
        for error in response.json()["details"]
    )
    mock_data_reader.read_data.assert_not_called()
