from fastapi import APIRouter, Cookie, Depends

from datastore_api.adapter import auth, db
from datastore_api.adapter.db.models import Datastore
from datastore_api.api import observability
from datastore_api.api.common.dependencies import get_datastore_id
from datastore_api.api.datastores import (
    data,
    importable_datasets,
    jobs,
    languages,
    metadata,
    targets,
)
from datastore_api.api.datastores.models import NewDatastoreRequest
from datastore_api.api.datastores.setup_datastore import setup_datastore
from datastore_api.common.exceptions import (
    DatastoreExistsException,
    DatastorePathExistsException,
)

router = APIRouter()


@router.get("")
async def get_datastores(
    db_client: db.DatabaseClient = Depends(db.get_database_client),
) -> list[str]:
    return db_client.get_datastores()


@router.post("")
async def create_new_datastore(
    validated_body: NewDatastoreRequest,
    authorization: str | None = Cookie(None),
    user_info: str | None = Cookie(None, alias="user-info"),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
    db_client: db.DatabaseClient = Depends(db.get_database_client),
) -> None:
    auth_client.authorize_datastore_modification(authorization, user_info)
    if validated_body.rdn in db_client.get_datastores():
        raise DatastoreExistsException("Datastore already exists")
    if validated_body.directory in db_client.get_datastore_dirs():
        raise DatastorePathExistsException("Datastore directory already exists")
    db_client.new_datastore(validated_body)
    setup_datastore(validated_body)
    # TODO: create new job for generating the RSA-keys


@router.get("/{datastore_rdn}")
async def get_datastore(
    db_client: db.DatabaseClient = Depends(db.get_database_client),
    datastore_id: int = Depends(get_datastore_id),
) -> Datastore:
    return db_client.get_datastore(datastore_id)


router.include_router(jobs.router, prefix="/{datastore_rdn}/jobs")
router.include_router(metadata.router, prefix="/{datastore_rdn}/metadata")
router.include_router(data.router, prefix="/{datastore_rdn}/data")
router.include_router(
    importable_datasets.router, prefix="/{datastore_rdn}/importable-datasets"
)
router.include_router(targets.router, prefix="/{datastore_rdn}/targets")
router.include_router(languages.router, prefix="/{datastore_rdn}/languages")

router.include_router(observability.router, prefix="/{datastore_rdn}/health")
