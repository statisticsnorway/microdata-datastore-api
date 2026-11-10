import pytest
from fastapi.testclient import TestClient

from datastore_api.adapter.auth.dependencies import authorize_user
from datastore_api.common.exceptions import (
    AuthError,
    AuthProviderUnavailableException,
)
from datastore_api.main import app


@pytest.fixture
def client():
    def _raise_auth_error() -> None:
        raise AuthError("Invalid token")

    app.dependency_overrides[authorize_user] = _raise_auth_error
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_invalid_auth_token_returns_401(client: TestClient):
    response = client.post(
        "/datastores/no.ssb.test/data/event/stream",
        json={
            "version": "1.0.0.0",
            "dataStructureName": "FAKE_NAME",
            "startDate": 0,
            "stopDate": 0,
        },
        headers={"Authorization": "Bearer bad-token"},
    )

    assert response.status_code == 401
    assert response.json() == {
        "type": "AUTHENTICATION_FAILURE",
        "code": 104,
        "message": "The provided JWT was not accepted",
    }


def test_auth_provider_unavailable_returns_system_error():
    def _raise_auth_provider_unavailable() -> None:
        raise AuthProviderUnavailableException("HTTP Error 502: Bad Gateway")

    app.dependency_overrides[authorize_user] = _raise_auth_provider_unavailable
    response = TestClient(app).post(
        "/datastores/no.ssb.test/data/event/stream",
        json={
            "version": "1.0.0.0",
            "dataStructureName": "FAKE_NAME",
            "startDate": 0,
            "stopDate": 0,
        },
        headers={"Authorization": "Bearer token"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 500
    assert response.json() == {
        "type": "SYSTEM_ERROR",
        "code": 202,
        "message": "An unexpected system error occured.",
    }
