import logging
from pathlib import Path

from datastore_api.adapter import db
from datastore_api.adapter.local_storage import setup_datastore
from datastore_api.common.exceptions import (
    DatastoreExistsException,
    DatastorePathExistsException,
    DatastoreSetupException,
)
from datastore_api.config import environment
from datastore_api.domain.datastores.models import NewDatastore

DATASTORES_ROOT_DIR = environment.datastores_root_dir

logger = logging.getLogger()


def get_datastore_dir_from_rdn(rdn: str) -> str:
    root_dir = Path(DATASTORES_ROOT_DIR)
    dir_name = rdn.replace(".", "-").lower()
    datastore_dir = root_dir / dir_name
    if not datastore_dir.resolve().is_relative_to(root_dir.resolve()):
        raise ValueError(
            "Resolved datastore directory is outside allowed base directory"
        )
    return str(datastore_dir)


def create_new_datastore(
    new_datastore: NewDatastore, db_client: db.DatabaseClient
) -> None:
    if new_datastore.rdn in db_client.get_datastores():
        raise DatastoreExistsException(
            f"Datastore rdn {new_datastore.rdn} already exists"
        )
    if Path(new_datastore.directory).exists():
        logger.error(
            f"Datastore directory exists ({new_datastore.directory}) "
            f"without matching rdn ({new_datastore.rdn}) in the database."
        )
        raise DatastorePathExistsException(
            f"Datastore directory already exists at {new_datastore.directory}"
        )
    db_client.insert_new_datastore(
        rdn=new_datastore.rdn,
        description=new_datastore.description,
        directory=new_datastore.directory,
        name=new_datastore.name,
        bump_enabled=new_datastore.bump_enabled,
    )
    try:
        setup_datastore(
            rdn=new_datastore.rdn,
            description=new_datastore.description,
            directory=new_datastore.directory,
            name=new_datastore.name,
        )
    except Exception as e:
        db_client.hard_delete_datastore(rdn=new_datastore.rdn)
        raise DatastoreSetupException(
            f"Failed to set up datastore {new_datastore.rdn}"
        ) from e
