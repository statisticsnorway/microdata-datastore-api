from unittest.mock import Mock

import pytest

from datastore_api.adapter.db.models import Datastore
from datastore_api.adapter.local_storage import datastore_directory
from datastore_api.common.models import Version

TEST_DIR = "tests/resources/test_datastore"
TEST_DATA_DIR = f"{TEST_DIR}/data"
TEST_PERSON_INCOME_PATH = (
    f"{TEST_DATA_DIR}/TEST_PERSON_INCOME/TEST_PERSON_INCOME"
)
TEST_PERSON_INCOME_PATH_1_0 = f"{TEST_PERSON_INCOME_PATH}__1_0.parquet"
TEST_PERSON_INCOME_PATH_DRAFT = f"{TEST_PERSON_INCOME_PATH}__DRAFT.parquet"
TEST_STUDIEPOENG_PATH_1_0 = (
    f"{TEST_DATA_DIR}/TEST_STUDIEPOENG/TEST_STUDIEPOENG__1_0"
)

DATABASE_RESPONSE_OBJECT = Datastore(
    rdn="no.dev.test",
    description="datastore for testing",
    directory="tests/resources/test_datastore",
    name="Test datastore",
    bump_enabled=False,
)


@pytest.fixture
def mock_db_client():
    mock = Mock()
    mock.get_datastore.return_value = DATABASE_RESPONSE_OBJECT
    return mock


@pytest.fixture
def patch_db(mock_db_client, monkeypatch):
    monkeypatch.setattr(
        datastore_directory.db, "get_database_client", lambda: mock_db_client
    )


def test_get_file_path(patch_db):
    assert TEST_PERSON_INCOME_PATH_1_0 == (
        datastore_directory.get_data_path_from_data_versions(
            "TEST_PERSON_INCOME", Version.from_str("1.0.0.0")
        )
    )


def test_get_file_path_draft(patch_db):
    assert TEST_PERSON_INCOME_PATH_DRAFT == (
        datastore_directory.get_draft_data_file_path("TEST_PERSON_INCOME")
    )


def test_get_latest_in_draft_version(patch_db):
    assert (
        datastore_directory.get_draft_data_file_path("TEST_STUDIEPOENG") is None
    )


def test_get_latest_version(patch_db):
    assert Version.from_str("2.0.0.0") == (
        datastore_directory.get_latest_version()
    )


def test_get_partitioned_file_path(patch_db):
    assert TEST_STUDIEPOENG_PATH_1_0 == (
        datastore_directory.get_data_path_from_data_versions(
            "TEST_STUDIEPOENG", Version.from_str("1.0.0.0")
        )
    )
