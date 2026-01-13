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
    public_vault,
    targets,
)
from datastore_api.api.datastores.models import (
    NewDatastoreRequest,
)
from datastore_api.domain.datastores import (
    create_new_datastore,
)

router = APIRouter()


@router.get("")
async def get_datastores(
    db_client: db.DatabaseClient = Depends(db.get_database_client),
) -> list[str]:
    return db_client.get_datastores()


@router.post("")
async def new_datastore(
    validated_body: NewDatastoreRequest,
    authorization: str | None = Cookie(None),
    user_info: str | None = Cookie(None, alias="user-info"),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
    db_client: db.DatabaseClient = Depends(db.get_database_client),
) -> None:
    auth_client.authorize_datastore_modification(authorization, user_info)
    new_datastore = validated_body.generate_new_datastore_from_request()
    create_new_datastore(new_datastore, db_client)


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
router.include_router(public_vault.router, prefix="/{datastore_rdn}/public-key")

router.include_router(observability.router, prefix="/{datastore_rdn}/health")
