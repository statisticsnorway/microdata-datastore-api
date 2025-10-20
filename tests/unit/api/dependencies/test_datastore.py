from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from httpx import Response

from datastore_api.adapter import db
from datastore_api.main import app


@pytest.fixture
def mock_db_client():
    mock = Mock()
    mock.get_datastore.return_value = SimpleNamespace(
        directory=str("tests/resources/test_datastore")
    )
    mock.get_datastore_id_from_rdn.return_value = 3
    return mock


@pytest.fixture
def client(mock_db_client: Mock):
    app.dependency_overrides[db.get_database_client] = lambda: mock_db_client
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def _reset_mocks(mock_db_client):
    mock_db_client.reset_mock()
    yield
    mock_db_client.reset_mock()


def test_get_datastore_id_reads_rdn_from_path(client, mock_db_client):
    response: Response = client.get(
        "/datastores/no.ssb.test/metadata/data-store",
        headers={
            "X-Request-ID": "test-123",
            "Accept-Language": "no",
            "Accept": "application/json",
        },
    )
    assert response.status_code == 200
    mock_db_client.get_datastore_id_from_rdn.assert_called_once_with(
        "no.ssb.test"
    )
    mock_db_client.get_datastore.assert_called_once_with(3)

# TODO: Remove test once legacy routers without rdn is removed
def test_legacy_path_defaults_to_id_1(client, mock_db_client):
    response: Response = client.get(
        "/metadata/data-store",
        headers={
            "X-Request-ID": "test-123",
            "Accept-Language": "no",
            "Accept": "application/json",
        },
    )
    assert response.status_code == 200
    mock_db_client.get_datastore_id_from_rdn.assert_not_called()
    mock_db_client.get_datastore.assert_called_once_with(1)
