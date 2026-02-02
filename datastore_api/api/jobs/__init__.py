import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query

from datastore_api.adapter import db
from datastore_api.adapter.auth.dependencies import require_api_key
from datastore_api.adapter.db.models import Job, JobStatus, Operation
from datastore_api.api.jobs.models import (
    UpdateJobRequest,
)

logger = logging.getLogger()

router = APIRouter()


@router.get(
    "",
    response_model_exclude_none=True,
    dependencies=[Depends(require_api_key)],
)
def get_jobs(
    status: Optional[str] = Query(None),
    operation: Optional[str] = Query(None),
    ignoreCompleted: bool = Query(False),
    database_client: db.DatabaseClient = Depends(db.get_database_client),
) -> list[Job]:
    return database_client.get_jobs(
        datastore_id=None,
        status=JobStatus(status) if status else None,
        operations=[Operation(op) for op in operation.split(",")]
        if operation is not None
        else None,
        ignore_completed=ignoreCompleted,
    )


@router.get(
    "/{job_id}",
    response_model_exclude_none=True,
    dependencies=[Depends(require_api_key)],
)
def get_job(
    job_id: str,
    database_client: db.DatabaseClient = Depends(db.get_database_client),
) -> Job:
    return database_client.get_job(job_id)


@router.put("/{job_id}", dependencies=[Depends(require_api_key)])
def update_job(
    job_id: str,
    validated_body: UpdateJobRequest,
    database_client: db.DatabaseClient = Depends(db.get_database_client),
) -> dict:
    job = database_client.update_job(
        job_id,
        validated_body.status,
        validated_body.description,
        validated_body.log,
    )
    database_client.update_target(job)
    if (
        job.parameters.target == "DATASTORE"
        and job.status == "completed"
        and job.parameters.operation == Operation.BUMP
    ):
        database_client.update_bump_targets(job)
    return {"message": f"Updated job with jobId {job_id}"}
