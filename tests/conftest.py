import os

os.environ["DOCKER_HOST_NAME"] = "localhost"
os.environ["STACK"] = "test"
os.environ["COMMIT_ID"] = "abc123"
os.environ["SQLITE_URL"] = "test.db"
os.environ["JWT_AUTH"] = "OFF"
os.environ["JWKS_URL"] = "http://localhost"
os.environ["ACCESS_CONTROL_FILE"] = (
    "tests/resources/access_control/allowed_users.json"
)

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
