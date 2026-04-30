from pathlib import Path
from typing import Optional

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


def _create_importable(
    release_status: Optional[str],
    importable: ImportableDataset,
) -> Optional[ImportableModel]:
    if importable.has_data and release_status is None:
        return ImportableModel(
            dataset_name=importable.dataset_name,
            operation="ADD",
            is_archived=importable.is_archived,
        )
    if release_status is None:
        return None
    if release_status in ("DRAFT", "PENDING_RELEASE", "PENDING_DELETE"):
        return ImportableModel(
            dataset_name=importable.dataset_name,
            operation="-",
            is_archived=importable.is_archived,
        )
    if importable.has_data and release_status == "DELETED":
        return ImportableModel(
            dataset_name=importable.dataset_name,
            operation="ADD",
            is_archived=importable.is_archived,
        )
    if importable.has_data and release_status == "RELEASED":
        return ImportableModel(
            dataset_name=importable.dataset_name,
            operation="CHANGE",
            is_archived=importable.is_archived,
        )
    if not importable.has_data and release_status == "RELEASED":
        return ImportableModel(
            dataset_name=importable.dataset_name,
            operation="PATCH_METADATA",
            is_archived=importable.is_archived,
        )
    return None


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
