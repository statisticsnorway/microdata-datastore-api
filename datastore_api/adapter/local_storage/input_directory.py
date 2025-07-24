import logging
import os
import string
import tarfile
from pathlib import Path
from tarfile import ReadError

from datastore_api.common.exceptions import (
    NameValidationError,
    NotFoundException,
)
from datastore_api.common.models import CamelModel
from datastore_api.config import environment

logger = logging.getLogger()

INPUT_DIR = Path(environment.get("INPUT_DIR"))
ARCHIVE_DIR = INPUT_DIR / "archive"


class ImportableDataset(CamelModel, extra="forbid"):
    dataset_name: str
    has_metadata: bool
    has_data: bool
    is_archived: bool = False


def _has_data(tar: tarfile.TarFile) -> bool:
    return "chunks" in tar.getnames()


def _has_metadata(tar: tarfile.TarFile, dataset_name: str) -> bool:
    return f"{dataset_name}.json" in tar.getnames()


def _validate_dataset_name(dataset_name: str) -> bool:
    """
    Validates that the name of the dataset only contains valid
    characters (uppercase A-Z, numbers 0-9 and _)
    """

    invalid_leading_characters = string.digits + "_"
    valid_characters = string.ascii_uppercase + string.digits + "_"
    return dataset_name[0] not in invalid_leading_characters and all([
        character in valid_characters for character in dataset_name
    ])


def get_datasets_in_directory(
    dir_path: Path, is_archived: bool = False
) -> list[ImportableDataset]:
    datasets = []

    for item in os.listdir(dir_path):
        item_path = dir_path / item
        dataset_name, ext = os.path.splitext(item)
        try:
            if ext == ".tar" and tarfile.is_tarfile(item_path):
                tar = tarfile.open(item_path)
                importable_dataset = ImportableDataset(
                    dataset_name=dataset_name,
                    has_data=_has_data(tar),
                    has_metadata=_has_metadata(tar, dataset_name),
                    is_archived=is_archived,
                )
                if importable_dataset.has_metadata:
                    datasets.append(importable_dataset)
        except ReadError as e:
            logger.warning(
                f"Couldn't read tarfile for {dataset_name}: {str(e)}"
            )
            continue
    return [
        dataset
        for dataset in datasets
        if _validate_dataset_name(dataset.dataset_name)
    ]


def get_importable_datasets() -> list[ImportableDataset]:
    """
    Returns names of all valid datasets in input directory.
    """
    datasets = get_datasets_in_directory(INPUT_DIR)
    if ARCHIVE_DIR.exists():
        datasets += get_datasets_in_directory(ARCHIVE_DIR, is_archived=True)
    return datasets


def delete_importable_datasets(dataset_name: str) -> None:
    if not _validate_dataset_name(dataset_name):
        raise NameValidationError(
            f'"{dataset_name}" contains invalid characters. '
            'Please use only uppercase A-Z, numbers 0-9 or "_"'
        )
    try:
        os.remove(f"{INPUT_DIR}/{dataset_name}.tar")
    except (FileNotFoundError, OSError) as e:
        raise NotFoundException(f"File {dataset_name} not found") from e
