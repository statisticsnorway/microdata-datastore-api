import os


os.environ["DATASTORE_ROOT_DIR"] = "tests/resources/test_datastore"
os.environ["DOCKER_HOST_NAME"] = "localhost"
os.environ["COMMIT_ID"] = "abc123"

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
