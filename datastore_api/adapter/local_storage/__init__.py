import json
import logging
from pathlib import Path

from datastore_api.common.exceptions import (
    DatastorePathExistsException,
    StartUpException,
)
from datastore_api.common.models import CamelModel
from datastore_api.config import environment

logger = logging.getLogger()


def _create_datastore_file_structure(datastore_directory: str) -> None:
    root_dir = Path(datastore_directory)

    directories = [
        "data",
        "datastore",
        "vault",
    ]
    for directory in directories:
        (root_dir / directory).mkdir(parents=True, exist_ok=True)

    (root_dir.with_name(root_dir.name + "_input")).mkdir(
        parents=True, exist_ok=True
    )
    (root_dir.with_name(root_dir.name + "_working")).mkdir(
        parents=True, exist_ok=True
    )


def _create_metadata_all_draft(name: str, rdn: str, description: str) -> dict:
    return {
        "dataStore": {
            "name": rdn,
            "label": name,
            "description": description,
            "languageCode": "no",
        },
        "languages": [{"code": "no", "label": "Norsk"}],
        "dataStructures": [],
    }


def _create_draft_version() -> dict:
    return {
        "version": "0.0.0.0",
        "description": "Draft",
        "releaseTime": 0,
        "languageCode": "no",
        "dataStructureUpdates": [],
        "updateType": "",
    }


def _create_datastore_versions(name: str, rdn: str, description: str) -> dict:
    return {
        "name": rdn,
        "label": name,
        "description": description,
        "versions": [],
    }


def _save_json_file(path: Path, filename: str, json_dict: dict) -> None:
    file = path / filename
    with open(file, "w") as f:
        json.dump(json_dict, f, indent=2)


def setup_datastore(
    directory: str, name: str, rdn: str, description: str
) -> None:
    if Path(directory).exists():
        raise DatastorePathExistsException(
            f"Datastore already exists at {directory}"
        )
    _create_datastore_file_structure(directory)
    _save_json_file(
        path=Path(directory) / "datastore",
        filename="metadata_all__DRAFT.json",
        json_dict=_create_metadata_all_draft(
            name=name,
            description=description,
            rdn=rdn,
        ),
    )
    _save_json_file(
        path=Path(directory) / "datastore",
        filename="draft_version.json",
        json_dict=_create_draft_version(),
    )
    _save_json_file(
        path=Path(directory) / "datastore",
        filename="datastore_versions.json",
        json_dict=_create_datastore_versions(
            name=name,
            description=description,
            rdn=rdn,
        ),
    )
    logger.info("Datastore setup complete")


class DatastoreBaseline(CamelModel):
    rdn: str
    description: str
    directory: str
    name: str
    bump_enabled: bool = False


class BaselineFile(CamelModel):
    datastores: list[DatastoreBaseline]


def read_baseline_file() -> BaselineFile | None:
    if environment.baseline_file is None:
        return None
    baseline_path = Path(environment.baseline_file)
    if not baseline_path.exists():
        raise StartUpException(
            f"Could not find baseline file at: {baseline_path}"
        )
    try:
        with baseline_path.open(encoding="utf-8") as f:
            return BaselineFile.model_validate(json.load(f))
    except Exception as e:
        raise StartUpException(
            f"Failed to read baseline file at: {baseline_path}"
        ) from e
