from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from datastore_api.adapter import db
from datastore_api.main import app

MAINTENANCE_STATUS_REQUEST_VALID = {"msg": "we upgrade chill", "paused": True}
MAINTENANCE_STATUS_REQUEST_NO_MSG = {"paused": True}
MAINTENANCE_STATUS_REQUEST_INVALID_PAUSE_VALUE = {
    "msg": "we upgrade chill",
    "paused": "Should not be a string",
}

NEW_STATUS = {
    "msg": "we upgrade chill",
    "paused": 1,
    "timestamp": "2023-08-31 16:26:27.575276",
}

RESPONSE_FROM_DB = [
    {
        "msg": "Today is 2023-08-31, we need to upgrade again",
        "paused": 1,
        "timestamp": "2023-08-31 16:26:27.575276",
    },
    {
        "msg": "Today is 2023-08-30, finished upgrading",
        "paused": 0,
        "timestamp": "2023-08-30 16:26:27.575276",
    },
    {
        "msg": "Today is 2023-08-29, we need to upgrade",
        "paused": 1,
        "timestamp": "2023-08-29 16:26:27.575276",
    },
]


@pytest.fixture
def mock_db_client():
    mock = Mock()
    mock.set_maintenance_status.return_value = NEW_STATUS
    mock.get_latest_maintenance_status.return_value = RESPONSE_FROM_DB[0]
    mock.get_maintenance_history.return_value = RESPONSE_FROM_DB
    return mock


@pytest.fixture
def client(mock_db_client):
    app.dependency_overrides[db.get_database_client] = lambda: mock_db_client
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_set_maintenance_status(client, mock_db_client):
    response = client.post(
        "/maintenance-statuses",
        json=MAINTENANCE_STATUS_REQUEST_VALID,
    )
    mock_db_client.set_maintenance_status.assert_called_once()
    assert response.status_code == 200
    assert response.json() == NEW_STATUS


def test_set_maintenance_status_with_no_msg(client, mock_db_client):
    response = client.post(
        "/maintenance-statuses",
        json=MAINTENANCE_STATUS_REQUEST_NO_MSG,
    )
    mock_db_client.set_maintenance_status.assert_not_called()
    assert response.status_code == 400
    assert response.json().get("details") is not None


def test_set_maintenance_status_with_invalid_paused(client, mock_db_client):
    response = client.post(
        "/maintenance-statuses",
        json=MAINTENANCE_STATUS_REQUEST_INVALID_PAUSE_VALUE,
    )
    mock_db_client.set_maintenance_status.assert_not_called()
    assert response.status_code == 400
    assert response.json().get("details") is not None


def test_get_maintenance_status(client, mock_db_client):
    response = client.get("/maintenance-statuses/latest")
    mock_db_client.get_latest_maintenance_status.assert_called_once()
    assert response.status_code == 200
    assert response.json() == RESPONSE_FROM_DB[0]


def test_get_maintenance_history(client, mock_db_client):
    response = client.get("/maintenance-statuses")
    mock_db_client.get_maintenance_history.assert_called_once()
    assert response.status_code == 200
    assert response.json() == RESPONSE_FROM_DB
