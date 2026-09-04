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
from datastore_api.domain.data import (
    generate_data_filter,
)
from datastore_api.main import app

FAKE_RESULT_FILE_NAME = "fake_result_file_name"
MOCK_RESULT = pq.read_table("tests/resources/results/mocked_result.parquet")


class FakeDataReader:
    def read_data(self, data_filter, *, row_cap=None):
        return MOCK_RESULT


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
def client(mock_db_client: Mock, mock_auth_deps: dict):
    app.dependency_overrides[db.get_database_client] = lambda: mock_db_client
    app.dependency_overrides[authorize_user] = lambda: mock_auth_deps["user"]()
    app.dependency_overrides[generate_data_filter] = lambda: None
    app.dependency_overrides[get_data_reader] = FakeDataReader
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
