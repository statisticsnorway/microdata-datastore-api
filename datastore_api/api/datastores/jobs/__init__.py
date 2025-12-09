import logging
from typing import Optional

from fastapi import APIRouter, Cookie, Depends, Query

from datastore_api.adapter import auth, db
from datastore_api.adapter.db.models import Job, JobStatus, Operation
from datastore_api.api.common.dependencies import (
    get_datastore_id,
)
from datastore_api.api.jobs.models import (
    NewJobResponse,
    NewJobsRequest,
)
from datastore_api.common.exceptions import (
    BumpingDisabledException,
    NotFoundException,
)

logger = logging.getLogger()

router = APIRouter()


@router.get("", response_model_exclude_none=True)
def get_jobs_for_datastore(
    status: Optional[str] = Query(None),
    operation: Optional[str] = Query(None),
    ignoreCompleted: bool = Query(False),
    database_client: db.DatabaseClient = Depends(db.get_database_client),
    datastore_id: int = Depends(get_datastore_id),
) -> list[Job]:
    return database_client.get_jobs(
        datastore_id=datastore_id,
        status=JobStatus(status) if status else None,
        operations=[Operation(op) for op in operation.split(",")]
        if operation is not None
        else None,
        ignore_completed=ignoreCompleted,
    )


@router.get("/{job_id}", response_model_exclude_none=True)
def get_job(
    job_id: str,
    datastore_rdn: str,
    authorization: str | None = Cookie(None),
    user_info: str | None = Cookie(None, alias="user-info"),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
    database_client: db.DatabaseClient = Depends(db.get_database_client),
) -> Job:
    auth_client.authorize_data_administrator(authorization, user_info)
    job = database_client.get_job(job_id)
    if job.datastore_rdn != datastore_rdn:
        raise NotFoundException
    return job


@router.post("", response_model_exclude_none=True)
def new_job(
    datastore_rdn: str,
    validated_body: NewJobsRequest,
    authorization: str | None = Cookie(None),
    user_info: str | None = Cookie(None, alias="user-info"),
    database_client: db.DatabaseClient = Depends(db.get_database_client),
    auth_client: auth.AuthClient = Depends(auth.get_auth_client),
    datastore_id: int = Depends(get_datastore_id),
) -> list[NewJobResponse]:
    parsed_user_info = auth_client.authorize_data_administrator(
        authorization, user_info
    )
    response_list = []
    for job_request in validated_body.jobs:
        try:
            if (
                job_request.target == "DATASTORE"
                and job_request.operation == "BUMP"
                and database_client.get_datastore(datastore_id).bump_enabled
                is False
            ):
                raise BumpingDisabledException(
                    "Bumping the datastore is disabled"
                )
            else:
                job = database_client.new_job(
                    job_request.generate_job_from_request(
                        "", parsed_user_info, datastore_rdn
                    )
                )
                response_list.append(
                    NewJobResponse(
                        status="queued",
                        msg="CREATED",
                        job_id=str(job.job_id),
                    )
                )
            database_client.update_target(job)
        except BumpingDisabledException as e:
            logger.exception(e)
            response_list.append(
                NewJobResponse(
                    status="FAILED",
                    msg="FAILED: Bumping the datastore is disabled",
                )
            )
        except Exception as e:
            logger.exception(e)
            response_list.append({"status": "FAILED", "msg": "FAILED"})
    return response_list
