import pytest
from fastapi.testclient import TestClient

from datastore_api.common.exceptions import MigrationException
from datastore_api.config import environment
from datastore_api.main import app, setup_db

client = TestClient(app)


def test_alive():
    response = client.get("/health/alive")
    assert response.status_code == 200
    assert response.text == '"I\'m alive!"'


def test_ready():
    response = client.get("/health/ready")
    assert response.status_code == 200
    assert response.text == '"I\'m ready!"'


def test_setup_db_invalid(tmp_path, monkeypatch):
    monkeypatch.setattr(environment, "sqlite_url", str(tmp_path / "test.db"))
    monkeypatch.setattr(
        environment,
        "migrations_dir",
        "tests/resources/migrations/invalid",
    )

    with pytest.raises(MigrationException):
        setup_db()
