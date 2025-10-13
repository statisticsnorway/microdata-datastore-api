import os

os.environ["DOCKER_HOST_NAME"] = "localhost"
os.environ["STACK"] = "test"
os.environ["COMMIT_ID"] = "abc123"
os.environ["SQLITE_URL"] = "test.db"
os.environ["JWT_AUTH"] = "false"
os.environ["JWKS_URL"] = "http://localhost"

import pytest
from fastapi import testclient

from datastore_api.main import app


@pytest.fixture(scope="session")
def test_app():
    yield testclient.TestClient(app)


def pytest_addoption(parser):
    parser.addoption(
        "--include-big-data",
        action="store_true",
        dest="include-big-data",
        default=False,
        help="enable big data testing",
    )
