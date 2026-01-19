import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from httpx import Response

from datastore_api.adapter import auth, db
from datastore_api.api.datastores.public_key import PUBLIC_KEY_FILE_NAME
from datastore_api.main import app


def generate_public_key():
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )

    public_key = private_key.public_key()

    microdata_public_key_pem_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return microdata_public_key_pem_bytes


PUBLIC_KEY_BYTES = generate_public_key()
PUBLIC_KEY_PATH = Path(
    f"tests/resources/test_datastore/vault/{PUBLIC_KEY_FILE_NAME}"
)


@pytest.fixture
def mock_auth_client():
    mock = Mock()
    mock.check_api_key.return_value = None
    return mock


@pytest.fixture
def mock_db_client():
    mock = Mock()
    mock.get_datastore.return_value = SimpleNamespace(
        directory=str("tests/resources/test_datastore")
    )
    mock.get_datastore_id_from_rdn.return_value = 1
    return mock


@pytest.fixture
def client(mock_auth_client: Mock, mock_db_client: Mock):
    app.dependency_overrides[auth.get_auth_client] = lambda: mock_auth_client
    app.dependency_overrides[db.get_database_client] = lambda: mock_db_client
    yield TestClient(app)
    app.dependency_overrides.clear()
    if os.path.exists(PUBLIC_KEY_PATH):
        os.remove(PUBLIC_KEY_PATH)


def test_save_public_key(client: TestClient):
    client.post(
        "/datastores/no.ssb.test/public-key",
        content=PUBLIC_KEY_BYTES,
        headers={"Content-Type": "application/x-pem-file", "X-API-Key": "abc"},
    )
    assert PUBLIC_KEY_PATH.is_file()


def test_create_key_already_exists(client: TestClient):
    PUBLIC_KEY_PATH.write_bytes(PUBLIC_KEY_BYTES)

    response = client.post(
        "/datastores/no.ssb.test/public-key",
        content=PUBLIC_KEY_BYTES,
        headers={"Content-Type": "application/x-pem-file", "X-API-Key": "abc"},
    )
    assert response.status_code == 409


def test_create_key_empty_bytes(client: TestClient):
    response: Response = client.post(
        "/datastores/no.ssb.test/public-key",
        content=b"",
        headers={"Content-Type": "application/x-pem-file", "X-API-Key": "abc"},
    )
    assert response.status_code == 400


def test_get_public_key(client: TestClient):
    PUBLIC_KEY_PATH.write_bytes(PUBLIC_KEY_BYTES)

    response: Response = client.get(
        "/datastores/no.ssb.test/public-key",
        headers={"Content-Type": "application/x-pem-file"},
    )
    assert response.status_code == 200
    assert response.content == PUBLIC_KEY_BYTES


def test_get_nonexisting_public_key(client: TestClient):
    response: Response = client.get(
        "/datastores/no.ssb.test/public-key",
        headers={"Content-Type": "application/x-pem-file"},
    )
    assert response.status_code == 404
    assert "Public key not found at" in response.text
