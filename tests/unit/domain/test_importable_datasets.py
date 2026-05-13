from unittest.mock import patch

import pytest

from datastore_api.adapter.local_storage.input_directory import (
    InputDirectoryTarFile,
)
from datastore_api.domain import importable_datasets
from datastore_api.domain.importable_datasets import (
    ImportableDataset,
    find_importables,
)


def _make_tar_file(
    has_data: bool,
    has_metadata: bool = True,
    is_archived: bool = False,
    dataset_name: str = "MY_DATASET",
) -> InputDirectoryTarFile:
    return InputDirectoryTarFile(
        dataset_name=dataset_name,
        has_data=has_data,
        has_metadata=has_metadata,
        is_archived=is_archived,
    )


@pytest.mark.parametrize(
    "release_status, has_data, expected_operation",
    [
        (None, True, "ADD"),
        (None, False, None),  # filtered out
        ("DRAFT", True, "-"),
        ("DRAFT", False, "-"),
        ("PENDING_RELEASE", True, "-"),
        ("PENDING_DELETE", False, "-"),
        ("DELETED", True, "ADD"),
        ("DELETED", False, None),  # filtered out
        ("RELEASED", True, "CHANGE"),
        ("RELEASED", False, "PATCH_METADATA"),
    ],
)
def test_from_tar_file_derives_operation_per_branch(
    release_status, has_data, expected_operation
):
    tar_file = _make_tar_file(has_data=has_data, dataset_name="DS")

    importable = ImportableDataset.from_tar_file(tar_file, release_status)

    if expected_operation is None:
        assert importable is None
    else:
        assert importable == ImportableDataset(
            dataset_name="DS",
            operation=expected_operation,
            is_archived=False,
        )


def test_from_tar_file_preserves_is_archived():
    tar_file = _make_tar_file(
        has_data=True, is_archived=True, dataset_name="ARCHIVED"
    )

    importable = ImportableDataset.from_tar_file(tar_file, None)

    assert importable is not None
    assert importable.is_archived is True


def test_find_importables_returns_empty_when_no_tar_files(tmp_path):
    with (
        patch.object(
            importable_datasets.input_directory,
            "get_importable_tar_files",
            return_value=[],
        ) as mock_get,
        patch.object(
            importable_datasets.metadata,
            "find_current_data_structure_status",
        ) as mock_status,
    ):
        result = find_importables(
            datastore_input_dir=tmp_path,
            datastore_root_dir=tmp_path,
            filter_out=[],
        )

    assert result == []
    mock_get.assert_called_once()
    mock_status.assert_not_called()


def test_find_importables_passes_filter_out(tmp_path):
    with patch.object(
        importable_datasets.input_directory,
        "get_importable_tar_files",
        return_value=[],
    ) as mock_get:
        find_importables(
            datastore_input_dir=tmp_path,
            datastore_root_dir=tmp_path,
            filter_out=["IN_PROGRESS"],
        )

    mock_get.assert_called_once_with(tmp_path, filter_out=["IN_PROGRESS"])


def test_find_importables_archived_datasets_with_data(tmp_path):
    tar_files = [
        _make_tar_file(
            has_data=False,
            is_archived=True,
            dataset_name="ARCHIVED_WITHOUT_DATA",
        ),
        _make_tar_file(
            has_data=True,
            is_archived=True,
            dataset_name="ARCHIVED_WITH_DATA",
        ),
    ]
    statuses = {"ARCHIVED_WITHOUT_DATA": None, "ARCHIVED_WITH_DATA": None}
    with (
        patch.object(
            importable_datasets.input_directory,
            "get_importable_tar_files",
            return_value=tar_files,
        ),
        patch.object(
            importable_datasets.metadata,
            "find_current_data_structure_status",
            return_value=statuses,
        ),
    ):
        result = find_importables(
            datastore_input_dir=tmp_path,
            datastore_root_dir=tmp_path,
            filter_out=[],
        )

    assert result == [
        ImportableDataset(
            dataset_name="ARCHIVED_WITH_DATA",
            operation="ADD",
            is_archived=True,
        )
    ]
