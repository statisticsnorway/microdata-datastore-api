from pathlib import Path

from datastore_api.adapter import db
from datastore_api.adapter.local_storage.setup_datastore import setup_datastore
from datastore_api.api.datastores.models import NewDatastoreRequest
from datastore_api.common.exceptions import (
    DatastoreExistsException,
    DatastorePathExistsException,
)
from datastore_api.config import environment
from datastore_api.domain.datastores.models import NewDatastore

DATASTORES_ROOT_DIR = environment.datastores_root_dir


def generate_datastore_dir_from_rdn(rdn: str) -> str:
    root_dir = Path(DATASTORES_ROOT_DIR)
    dir_name = rdn.replace(".", "_").lower()
    datastore_dir = root_dir / dir_name
    if not datastore_dir.resolve().is_relative_to(root_dir.resolve()):
        raise ValueError(
            "Resolved datastore directory is outside allowed base directory"
        )
    return str(datastore_dir)


def generate_new_datastore_from_request(
    request: NewDatastoreRequest,
) -> NewDatastore:
    datastore_dir = generate_datastore_dir_from_rdn(request.rdn)
    return NewDatastore(
        name=request.name,
        rdn=request.rdn,
        description=request.description,
        directory=datastore_dir,
        bump_enabled=request.bump_enabled,
    )


def create_new_datastore(
    new_datastore: NewDatastore, db_client: db.DatabaseClient
) -> None:
    if new_datastore.rdn in db_client.get_datastores():
        raise DatastoreExistsException("Datastore already exists")
    if Path(new_datastore.directory).exists():
        raise DatastorePathExistsException(
            f"Datastore already exists at {new_datastore.directory}"
        )
    db_client.new_datastore(new_datastore)
    setup_datastore(new_datastore)
