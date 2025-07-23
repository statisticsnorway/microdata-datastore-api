from unittest.mock import Mock

import pytest
from pytest import MonkeyPatch
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from datastore_api.main import app
from datastore_api.adapter import auth
from datastore_api.domain import data

FAKE_RESULT_FILE_NAME = "fake_result_file_name"
MOCK_RESULT = pq.read_table(
    "tests/resources/data_service/results/mocked_result.parquet"
)


@pytest.fixture
def mock_auth_client():
    mock = Mock()
    mock.authorize_user.return_value = ""
    return mock


@pytest.fixture
def client(mock_auth_client: Mock):
    app.dependency_overrides[auth.get_auth_client] = lambda: mock_auth_client
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def setup(monkeypatch: MonkeyPatch):
    for temporality in ["status", "event", "fixed"]:
        monkeypatch.setattr(
            data, f"process_{temporality}_request", lambda _: MOCK_RESULT
        )


def test_data_event_stream_result(client: TestClient):
    response = client.post(
        "/data/event/stream",
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
        "/data/status/stream",
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
        "/data/fixed/stream",
        json={"version": "1.0.0.0", "dataStructureName": "FAKE_NAME"},
        headers={"Authorization": "Bearer valid-token"},
    )

    reader = pa.BufferReader(response.content)
    assert response.status_code == 200
    assert pq.read_table(reader) == MOCK_RESULT
