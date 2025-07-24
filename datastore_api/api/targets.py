import logging

from fastapi import APIRouter, Depends

from datastore_api.adapter import db

logger = logging.getLogger()
router = APIRouter()


@router.get("/")
def get_targets(
    database_client: db.DatabaseClient = Depends(db.get_database_client),
):
    targets = database_client.get_targets()
    return [
        target.model_dump(exclude_none=True, by_alias=True)
        for target in targets
    ]


@router.get("/{name}/jobs")
def get_target_jobs(
    name: str,
    database_client: db.DatabaseClient = Depends(db.get_database_client),
):
    jobs = database_client.get_jobs_for_target(name)
    return [job.model_dump(exclude_none=True, by_alias=True) for job in jobs]
