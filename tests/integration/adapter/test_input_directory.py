import os
import shutil
from pathlib import Path

import pytest

from datastore_api.adapter.local_storage import input_directory
from datastore_api.adapter.local_storage.input_directory import (
    ImportableDataset,
)
from datastore_api.common.exceptions import (
    NameValidationError,
    NotFoundException,
)

INPUT_DIR = Path("tests/resources/test_datastore_input")


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
    actual_datasets = input_directory.get_importable_datasets(INPUT_DIR)
    assert len(actual_datasets) == 4
    for dataset in expected_datasets:
        assert dataset in actual_datasets


def test_get_importable_datasets_filter():
    actual_datasets = input_directory.get_importable_datasets(
        INPUT_DIR, ["YET_ANOTHER_DATASET"]
    )
    assert len(actual_datasets) == 3
    my_expected_datasets = expected_datasets[0:3]
    for dataset in my_expected_datasets:
        assert dataset in actual_datasets

    for dataset in actual_datasets:
        assert dataset in my_expected_datasets


def test_create_and_delete_importable_dataset():
    shutil.copyfile(
        f"{INPUT_DIR}/MY_DATASET.tar",
        f"{INPUT_DIR}/DATASET_FOR_DELETE.tar",
    )
    input_directory.delete_importable_datasets("DATASET_FOR_DELETE", INPUT_DIR)

    assert "DATASET_FOR_DELETE.tar" not in os.listdir(INPUT_DIR)


def test_delete_importable_datasets_raises_not_found_exception():
    with pytest.raises(NotFoundException):
        input_directory.delete_importable_datasets("NO_SUCH_DATASET", INPUT_DIR)


def test_delete_importable_datasets_raises_name_validation_error():
    with pytest.raises(NameValidationError):
        input_directory.delete_importable_datasets(
            "1_INVALID_DATASET_NAME", INPUT_DIR
        )
