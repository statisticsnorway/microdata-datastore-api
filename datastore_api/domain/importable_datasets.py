from pathlib import Path

from datastore_api.adapter.local_storage import input_directory
from datastore_api.adapter.local_storage.input_directory import (
    ImportableDataset,
)
from datastore_api.common.models import CamelModel
from datastore_api.domain import metadata


class ImportableModel(CamelModel):
    dataset_name: str
    operation: str
    selected: bool = False
    is_archived: bool


def _operation(release_status: str | None, has_data: bool) -> str | None:
    if release_status is None:
        return "ADD" if has_data else None
    if release_status in ("DRAFT", "PENDING_RELEASE", "PENDING_DELETE"):
        return "-"
    if release_status == "DELETED":
        return "ADD" if has_data else None
    if release_status == "RELEASED":
        return "CHANGE" if has_data else "PATCH_METADATA"
    return None


def _create_importable(
    release_status: str | None,
    importable: ImportableDataset,
) -> ImportableModel | None:
    operation = _operation(release_status, importable.has_data)
    if operation is None:
        return None
    return ImportableModel(
        dataset_name=importable.dataset_name,
        operation=operation,
        is_archived=importable.is_archived,
    )


def find_importables(
    datastore_input_dir: Path,
    datastore_root_dir: Path,
    filter_out: list[str],
) -> list[ImportableModel]:
    importable_datasets = input_directory.get_importable_datasets(
        datastore_input_dir, filter_out=filter_out
    )
    if not importable_datasets:
        return []
    dataset_names = [d.dataset_name for d in importable_datasets]
    statuses = metadata.find_current_data_structure_status(
        dataset_names, datastore_root_dir
    )
    result: list[ImportableModel] = []
    for importable in importable_datasets:
        status_entry = statuses.get(importable.dataset_name)
        release_status = status_entry["releaseStatus"] if status_entry else None
        importable_model = _create_importable(release_status, importable)
        if importable_model is not None:
            result.append(importable_model)
    return result
