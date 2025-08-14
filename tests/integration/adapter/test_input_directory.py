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


def test_get_importable_datasets():
    actual_datasets = input_directory.get_importable_datasets()
    assert len(actual_datasets) == 4
    for dataset in expected_datasets:
        assert dataset in actual_datasets
