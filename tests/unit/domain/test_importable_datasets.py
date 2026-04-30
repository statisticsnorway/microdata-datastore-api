from unittest.mock import patch

from datastore_api.adapter.local_storage.input_directory import (
    ImportableDataset,
)
from datastore_api.domain import importable_datasets
from datastore_api.domain.importable_datasets import (
    ImportableModel,
    _create_importable,
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


def test_create_importable_new_dataset_with_data():
    result = _create_importable(None, _make_importable(has_data=True))
    assert result is not None
    assert result.operation == "ADD"
    assert result.selected is False


def test_create_importable_new_dataset_without_data_is_filtered():
    result = _create_importable(None, _make_importable(has_data=False))
    assert result is None


def test_create_importable_draft_status():
    result = _create_importable("DRAFT", _make_importable(has_data=True))
    assert result is not None
    assert result.operation == "-"


def test_create_importable_pending_release_status():
    result = _create_importable(
        "PENDING_RELEASE", _make_importable(has_data=True)
    )
    assert result is not None
    assert result.operation == "-"


def test_create_importable_pending_delete_status():
    result = _create_importable(
        "PENDING_DELETE", _make_importable(has_data=False)
    )
    assert result is not None
    assert result.operation == "-"


def test_create_importable_deleted_with_data():
    result = _create_importable("DELETED", _make_importable(has_data=True))
    assert result is not None
    assert result.operation == "ADD"


def test_create_importable_deleted_without_data_is_filtered():
    result = _create_importable("DELETED", _make_importable(has_data=False))
    assert result is None


def test_create_importable_released_with_data():
    result = _create_importable("RELEASED", _make_importable(has_data=True))
    assert result is not None
    assert result.operation == "CHANGE"


def test_create_importable_released_without_data():
    result = _create_importable("RELEASED", _make_importable(has_data=False))
    assert result is not None
    assert result.operation == "PATCH_METADATA"


def test_create_importable_preserves_is_archived():
    result = _create_importable(
        None, _make_importable(has_data=True, is_archived=True)
    )
    assert result is not None
    assert result.is_archived is True


def test_create_importable_selected_defaults_false():
    result = _create_importable("RELEASED", _make_importable(has_data=True))
    assert result is not None
    assert result.selected is False


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


def test_find_importables_enriches_and_filters(tmp_path):
    raw = [
        _make_importable(has_data=True, dataset_name="A"),
        _make_importable(has_data=False, dataset_name="B"),
        _make_importable(has_data=True, dataset_name="C"),
    ]
    statuses = {
        "A": {
            "operation": "ADD",
            "releaseTime": 1,
            "releaseStatus": "RELEASED",
        },
        "B": None,
        "C": None,
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

    assert [r.dataset_name for r in result] == ["A", "C"]
    assert result[0].operation == "CHANGE"
    assert result[1].operation == "ADD"
    assert all(isinstance(r, ImportableModel) for r in result)


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
