from fastapi import APIRouter, Depends

from datastore_api.adapter import db
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

router = APIRouter()


@router.get("")
async def get_datastores(
    db_client: db.DatabaseClient = Depends(db.get_database_client),
) -> list[str]:
    return db_client.get_datastores()


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
