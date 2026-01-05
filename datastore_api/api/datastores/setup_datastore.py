import json
import logging
from pathlib import Path

from datastore_api.api.datastores.models import NewDatastoreRequest
from datastore_api.common.exceptions import DatastorePathExistsException

"""Creates the directory structure of a datastore"""

logger = logging.getLogger()


def create_file_structure(new_datastore: NewDatastoreRequest) -> None:
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


def create_metadata_all_draft(new_datastore: NewDatastoreRequest) -> dict:
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


def create_draft_version() -> dict:
    return {
        "version": "0.0.0.0",
        "description": "Draft",
        "releaseTime": 0,
        "languageCode": "no",
        "dataStructureUpdates": [],
        "updateType": "",
    }


def create_datastore_versions(new_datastore: NewDatastoreRequest) -> dict:
    return {
        "name": new_datastore.name,
        "label": new_datastore.rdn,
        "description": new_datastore.description,
        "versions": [],
    }


def save_json_file(path: str, filename: str, json_dict: dict) -> None:
    file = f"{path}/{filename}"
    with open(file, "w") as f:
        json.dump(json_dict, f, indent=2)


def setup_datastore(new_datastore: NewDatastoreRequest) -> None:
    if Path(new_datastore.directory).exists():
        raise DatastorePathExistsException(
            f"Datastore already exists at {new_datastore.directory}"
        )
    create_file_structure(new_datastore)
    save_json_file(
        path=new_datastore.directory,
        filename="metadata_all__DRAFT.json",
        json_dict=create_metadata_all_draft(new_datastore),
    )
    save_json_file(
        path=new_datastore.directory,
        filename="draft_version.json",
        json_dict=create_draft_version(),
    )
    save_json_file(
        path=new_datastore.directory,
        filename="datastore_versions.json",
        json_dict=create_datastore_versions(new_datastore),
    )
    logger.info("Datastore setup complete")
