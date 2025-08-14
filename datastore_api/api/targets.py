import logging

from fastapi import APIRouter, Depends

from datastore_api.adapter import db
from datastore_api.adapter.db.models import Job, Target

logger = logging.getLogger()
router = APIRouter()


@router.get("/", response_model_exclude_none=True)
def get_targets(
    database_client: db.DatabaseClient = Depends(db.get_database_client),
) -> list[Target]:
    return database_client.get_targets()


@router.get("/{name}/jobs", response_model_exclude_none=True)
def get_target_jobs(
    name: str,
    database_client: db.DatabaseClient = Depends(db.get_database_client),
) -> list[Job]:
    return database_client.get_jobs_for_target(name)
