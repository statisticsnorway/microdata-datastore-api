from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from datastore_api.adapter import db
from datastore_api.adapter.db.models import Datastore
from datastore_api.api.common import dependencies
from datastore_api.main import app

DATASTORE = Datastore(
    datastore_id=1,
    rdn="no.dev.test",
    description="Datastore for testing",
    directory="tests/resources/test_datastore",
    name="Test datastore",
    bump_enabled=False,
)


@pytest.fixture
def mock_db_client():
    mock = Mock()
    mock.get_datastore = Mock(side_effect=lambda datastore_id: DATASTORE)
    mock.get_datastores = Mock(side_effect=lambda: ["no.dev.test"])
    return mock


@pytest.fixture
def client(mock_db_client):
    app.dependency_overrides[db.get_database_client] = lambda: mock_db_client
    app.dependency_overrides[dependencies.get_datastore_id] = lambda: 1
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_get_datastore(client):
    response = client.get(
        "/datastores/no.dev.test", headers={"X-Request-ID": "abc123"}
    )
    assert response.status_code == 200
    assert response.json() == DATASTORE.model_dump()


def test_get_datastores(client):
    response = client.get("/datastores", headers={"X-Request-ID": "abc123"})
    assert response.status_code == 200
    assert response.json() == ["no.dev.test"]
