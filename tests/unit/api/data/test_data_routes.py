from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient
from pytest import MonkeyPatch

from datastore_api.adapter import auth, db
from datastore_api.domain import data
from datastore_api.main import app

FAKE_RESULT_FILE_NAME = "fake_result_file_name"
MOCK_RESULT = pq.read_table("tests/resources/results/mocked_result.parquet")


@pytest.fixture
def mock_auth_client():
    mock = Mock()
    mock.authorize_user.return_value = ""
    return mock


@pytest.fixture
def mock_db_client():
    mock = Mock()
    mock.get_datastore.return_value = SimpleNamespace(
        directory=str("tests/resources/test_datastore")
    )
    return mock


@pytest.fixture
def client(mock_auth_client: Mock, mock_db_client: Mock):
    app.dependency_overrides[auth.get_auth_client] = lambda: mock_auth_client
    app.dependency_overrides[db.get_database_client] = lambda: mock_db_client
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def setup(monkeypatch: MonkeyPatch):
    monkeypatch.setattr(
        data, "process_status_request", lambda a, b, c, d, e, f: MOCK_RESULT
    )
    monkeypatch.setattr(
        data, "process_event_request", lambda a, b, c, d, e, f, g: MOCK_RESULT
    )
    monkeypatch.setattr(
        data, "process_fixed_request", lambda a, b, c, d, e: MOCK_RESULT
    )


def test_data_event_stream_result(client: TestClient):
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

    reader = pa.BufferReader(response.content)
    assert response.status_code == 200
    assert pq.read_table(reader) == MOCK_RESULT


def test_data_status_stream_result(client: TestClient):
    response = client.post(
        "/datastores/no.ssb.test/data/status/stream",
        json={
            "version": "1.0.0.0",
            "dataStructureName": "FAKE_NAME",
            "date": 0,
        },
        headers={"Authorization": "Bearer valid-token"},
    )

    reader = pa.BufferReader(response.content)
    assert response.status_code == 200
    assert pq.read_table(reader) == MOCK_RESULT


def test_data_fixed_stream_result(client: TestClient):
    response = client.post(
        "/datastores/no.ssb.test/data/fixed/stream",
        json={"version": "1.0.0.0", "dataStructureName": "FAKE_NAME"},
        headers={"Authorization": "Bearer valid-token"},
    )

    reader = pa.BufferReader(response.content)
    assert response.status_code == 200
    assert pq.read_table(reader) == MOCK_RESULT
