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

logger = logging.getLogger()


class InputDirectoryTarFile(CamelModel, extra="forbid"):
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
    return dataset_name[0] not in invalid_leading_characters and all(
        [character in valid_characters for character in dataset_name]
    )


def get_datasets_in_directory(
    dir_path: Path,
    filter_out: list[str],
    is_archived: bool = False,
) -> list[InputDirectoryTarFile]:
    datasets = []
    invalid_tar_files = 0

    for item in os.listdir(dir_path):
        dataset_name, ext = os.path.splitext(item)
        if dataset_name in filter_out:
            continue
        if _validate_dataset_name(dataset_name):
            continue
        item_path = dir_path / item
        try:
            if ext == ".tar" and tarfile.is_tarfile(item_path):
                tar = tarfile.open(item_path)
                tar_file = InputDirectoryTarFile(
                    dataset_name=dataset_name,
                    has_data=_has_data(tar),
                    has_metadata=_has_metadata(tar, dataset_name),
                    is_archived=is_archived,
                )
                if tar_file.has_metadata:
                    datasets.append(tar_file)
        except ReadError:
            invalid_tar_files += 1
            continue
    if invalid_tar_files > 1:
        logger.warning(
            "Found invalid tar files in input directory"
        )
    return datasets


def get_importable_tar_files(
    input_dir: Path,
    filter_out: list[str] = [],
) -> list[InputDirectoryTarFile]:
    """
    Returns all valid tar files in the input directory.
    """
    archive_dir = input_dir / "archive"
    datasets = get_datasets_in_directory(input_dir, filter_out)
    if archive_dir.exists():
        datasets += get_datasets_in_directory(
            archive_dir, filter_out, is_archived=True
        )
    return datasets


def delete_importable_datasets(dataset_name: str, input_dir: Path) -> None:
    if not _validate_dataset_name(dataset_name):
        raise NameValidationError(
            f'"{dataset_name}" contains invalid characters. '
            'Please use only uppercase A-Z, numbers 0-9 or "_"'
        )
    try:
        file_path = (input_dir / f"{dataset_name}.tar").resolve()
        if input_dir.resolve() not in file_path.parents:
            raise NameValidationError(
                f'Invalid path for dataset name "{dataset_name}"'
            )
        os.remove(file_path)
    except (FileNotFoundError, OSError) as e:
        raise NotFoundException(f"File {dataset_name} not found") from e
