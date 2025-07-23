import logging
from typing import Optional

from fastapi import APIRouter, Query, Cookie, Depends

from datastore_api.adapter import auth
from datastore_api.config import environment
from datastore_api.common.exceptions import BumpingDisabledException
from datastore_api.adapter.db.models import JobStatus, Operation
from datastore_api.api.jobs.models import (
    NewJobsRequest,
    UpdateJobRequest,
)
from datastore_api.adapter import db

logger = logging.getLogger()

router = APIRouter()


@router.get("/jobs")
def get_jobs(
    status: Optional[str] = Query(None),
    operation: Optional[str] = Query(None),
    ignoreCompleted: bool = Query(False),
    database_client: db.DatabaseClient = Depends(db.get_database_client),
):
    return [
        job.model_dump(exclude_none=True, by_alias=True)
        for job in database_client.get_jobs(
            status=JobStatus(status) if status else None,
            operations=[Operation(op) for op in operation.split(",")]
            if operation is not None
            else None,
            ignore_completed=ignoreCompleted,
        )
    ]


@router.post("/jobs")
def new_job(
    validated_body: NewJobsRequest,
    authorization: str | None = Cookie(None),
    user_info: str | None = Cookie(None),
    database_client: db.DatabaseClient = Depends(db.get_database_client),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
):
    parsed_user_info = auth_client.authorize_data_administrator(
        authorization, user_info
    )
    response_list = []
    for job_request in validated_body.jobs:
        try:
            if (
                job_request.target == "DATASTORE"
                and job_request.operation == "BUMP"
                and environment.get("BUMP_ENABLED") is False
            ):
                raise BumpingDisabledException(
                    "Bumping the datastore is disabled"
                )
            else:
                job = database_client.new_job(
                    job_request.generate_job_from_request("", parsed_user_info)
                )
                response_list.append(
                    {
                        "status": "queued",
                        "msg": "CREATED",
                        "job_id": job.job_id,
                    }
                )
            database_client.update_target(job)
        except BumpingDisabledException as e:
            logger.exception(e)
            response_list.append(
                {
                    "status": "FAILED",
                    "msg": "FAILED: Bumping the datastore is disabled",
                }
            )
        except Exception as e:
            logger.exception(e)
            response_list.append({"status": "FAILED", "msg": "FAILED"})
    return response_list


@router.get("/jobs/{job_id}")
def get_job(
    job_id: str,
    database_client: db.DatabaseClient = Depends(db.get_database_client),
):
    return database_client.get_job(job_id).model_dump(
        exclude_none=True, by_alias=True
    )


@router.put("/jobs/{job_id}")
def update_job(
    job_id: str,
    validated_body: UpdateJobRequest,
    database_client: db.DatabaseClient = Depends(db.get_database_client),
):
    job = database_client.update_job(
        job_id,
        validated_body.status,
        validated_body.description,
        validated_body.log,
    )
    database_client.update_target(job)
    if job.parameters.target == "DATASTORE" and job.status == "completed":
        database_client.update_bump_targets(job)
    return {"message": f"Updated job with jobId {job_id}"}
