import pytest

from datastore_api.main import app
from fastapi import testclient


@pytest.fixture(scope="session")
def test_app():
    yield testclient.TestClient(app)
