from unittest.mock import patch

import pytest

from datastore_api.adapter.local_storage.input_directory import (
    ImportableDataset,
)
from datastore_api.domain import importable_datasets
from datastore_api.domain.importable_datasets import (
    ImportableModel,
    find_importables,
)


def _make_importable(
    has_data: bool,
    has_metadata: bool = True,
    is_archived: bool = False,
    dataset_name: str = "MY_DATASET",
) -> ImportableDataset:
    return ImportableDataset(
        dataset_name=dataset_name,
        has_data=has_data,
        has_metadata=has_metadata,
        is_archived=is_archived,
    )


def _status(release_status: str) -> dict:
    return {
        "operation": "ADD",
        "releaseTime": 1,
        "releaseStatus": release_status,
    }


def test_find_importables_returns_empty_when_no_importables(tmp_path):
    with (
        patch.object(
            importable_datasets.input_directory,
            "get_importable_datasets",
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
        "get_importable_datasets",
        return_value=[],
    ) as mock_get:
        find_importables(
            datastore_input_dir=tmp_path,
            datastore_root_dir=tmp_path,
            filter_out=["IN_PROGRESS"],
        )
    mock_get.assert_called_once_with(tmp_path, filter_out=["IN_PROGRESS"])


def test_find_importables_preserves_is_archived(tmp_path):
    raw = [
        _make_importable(
            has_data=True, is_archived=True, dataset_name="ARCHIVED"
        )
    ]
    with (
        patch.object(
            importable_datasets.input_directory,
            "get_importable_datasets",
            return_value=raw,
        ),
        patch.object(
            importable_datasets.metadata,
            "find_current_data_structure_status",
            return_value={"ARCHIVED": None},
        ),
    ):
        result = find_importables(
            datastore_input_dir=tmp_path,
            datastore_root_dir=tmp_path,
            filter_out=[],
        )
    assert len(result) == 1
    assert result[0].is_archived is True
    assert result[0].selected is False


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
def test_find_importables_derives_operation_per_branch(
    tmp_path, release_status, has_data, expected_operation
):
    raw = [_make_importable(has_data=has_data, dataset_name="DS")]
    statuses = {
        "DS": _status(release_status) if release_status is not None else None
    }
    with (
        patch.object(
            importable_datasets.input_directory,
            "get_importable_datasets",
            return_value=raw,
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

    if expected_operation is None:
        assert result == []
    else:
        assert len(result) == 1
        assert isinstance(result[0], ImportableModel)
        assert result[0].dataset_name == "DS"
        assert result[0].operation == expected_operation
