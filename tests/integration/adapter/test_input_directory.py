from unittest.mock import Mock

import pytest

from datastore_api.adapter.db.models import Datastore
from datastore_api.adapter.local_storage import input_directory
from datastore_api.adapter.local_storage.input_directory import (
    ImportableDataset,
)

expected_datasets = [
    ImportableDataset(
        dataset_name="MY_DATASET", has_data=True, has_metadata=True
    ),
    ImportableDataset(
        dataset_name="YOUR_DATASET", has_data=False, has_metadata=True
    ),
    ImportableDataset(
        dataset_name="OTHER_DATASET", has_data=True, has_metadata=True
    ),
    ImportableDataset(
        dataset_name="YET_ANOTHER_DATASET",
        has_data=True,
        has_metadata=True,
        is_archived=True,
    ),
]
DATASTORE = Datastore(
    rdn="no.dev.test",
    description="Datastore for testing",
    directory="tests/resources/test_datastore",
    name="Test datastore",
    bump_enabled=True,
)


@pytest.fixture
def mock_db_client():
    mock = Mock()
    mock.get_datastore.return_value = DATASTORE
    return mock


def test_get_importable_datasets(mock_db_client):
    actual_datasets = input_directory.get_importable_datasets(mock_db_client)
    assert len(actual_datasets) == 4
    for dataset in expected_datasets:
        assert dataset in actual_datasets
