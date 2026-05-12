from pathlib import Path

from datastore_api.adapter.local_storage import input_directory
from datastore_api.adapter.local_storage.input_directory import (
    InputDirectoryTarFile,
)
from datastore_api.common.models import CamelModel
from datastore_api.domain import metadata


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


class ImportableDataset(CamelModel, extra="forbid"):
    dataset_name: str
    operation: str
    is_archived: bool

    @staticmethod
    def from_tar_file(
        tar_file: InputDirectoryTarFile,
        release_status: str | None,
    ) -> "ImportableDataset | None":
        operation = _operation(release_status, tar_file.has_data)
        if operation is None:
            return None
        return ImportableDataset(
            dataset_name=tar_file.dataset_name,
            operation=operation,
            is_archived=tar_file.is_archived,
        )


def find_importables(
    datastore_input_dir: Path,
    datastore_root_dir: Path,
    filter_out: list[str],
) -> list[ImportableDataset]:
    tar_files = input_directory.get_importable_tar_files(
        datastore_input_dir, filter_out=filter_out
    )
    if not tar_files:
        return []
    statuses = metadata.find_current_data_structure_status(
        [t.dataset_name for t in tar_files], datastore_root_dir
    )
    importables = [
        ImportableDataset.from_tar_file(
            tar_file,
            (statuses.get(tar_file.dataset_name) or {}).get("releaseStatus"),
        )
        for tar_file in tar_files
    ]
    return [importable for importable in importables if importable is not None]
