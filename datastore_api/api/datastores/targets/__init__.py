import logging

from fastapi import APIRouter, Depends

from datastore_api.adapter import db
from datastore_api.adapter.auth.dependencies import require_data_administrator
from datastore_api.adapter.db.models import Job, Target
from datastore_api.api.common.dependencies import (
    get_datastore_id,
)

logger = logging.getLogger()
router = APIRouter()


@router.get(
    "",
    response_model_exclude_none=True,
    dependencies=[Depends(require_data_administrator)],
)
def get_targets(
    database_client: db.DatabaseClient = Depends(db.get_database_client),
    datastore_id: int = Depends(get_datastore_id),
) -> list[Target]:
    return database_client.get_targets(datastore_id)


@router.get(
    "/{name}/jobs",
    response_model_exclude_none=True,
    dependencies=[Depends(require_data_administrator)],
)
def get_target_jobs(
    name: str,
    database_client: db.DatabaseClient = Depends(db.get_database_client),
    datastore_id: int = Depends(get_datastore_id),
) -> list[Job]:
    return database_client.get_jobs_for_target(
        name=name, datastore_id=datastore_id
    )
