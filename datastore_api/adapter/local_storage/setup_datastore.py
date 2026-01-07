import json
import logging
from pathlib import Path

from datastore_api.common.exceptions import DatastorePathExistsException
from datastore_api.domain.datastores.models import NewDatastore

"""Creates the directory structure of a datastore"""

logger = logging.getLogger()


def _create_file_structure(new_datastore: NewDatastore) -> None:
    root_dir = Path(new_datastore.directory)

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


def _create_metadata_all_draft(new_datastore: NewDatastore) -> dict:
    return {
        "dataStore": {
            "name": new_datastore.name,
            "label": new_datastore.rdn,
            "description": new_datastore.description,
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


def _create_datastore_versions(new_datastore: NewDatastore) -> dict:
    return {
        "name": new_datastore.name,
        "label": new_datastore.rdn,
        "description": new_datastore.description,
        "versions": [],
    }


def _save_json_file(path: Path, filename: str, json_dict: dict) -> None:
    file = path / filename
    with open(file, "w") as f:
        json.dump(json_dict, f, indent=2)


def setup_datastore(new_datastore: NewDatastore) -> None:
    if Path(new_datastore.directory).exists():
        raise DatastorePathExistsException(
            f"Datastore already exists at {new_datastore.directory}"
        )
    _create_file_structure(new_datastore)
    _save_json_file(
        path=Path(new_datastore.directory),
        filename="metadata_all__DRAFT.json",
        json_dict=_create_metadata_all_draft(new_datastore),
    )
    _save_json_file(
        path=Path(new_datastore.directory),
        filename="draft_version.json",
        json_dict=_create_draft_version(),
    )
    _save_json_file(
        path=Path(new_datastore.directory),
        filename="datastore_versions.json",
        json_dict=_create_datastore_versions(new_datastore),
    )
    logger.info("Datastore setup complete")
