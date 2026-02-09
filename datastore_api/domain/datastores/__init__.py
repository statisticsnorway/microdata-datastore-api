import logging
from datetime import datetime
from pathlib import Path

from datastore_api.adapter import db
from datastore_api.adapter.db import Operation
from datastore_api.adapter.db.models import (
    Job,
    JobParameters,
    JobStatus,
    UserInfo,
)
from datastore_api.adapter.local_storage import setup_datastore
from datastore_api.api.jobs.models import NewJobResponse
from datastore_api.common.exceptions import (
    DatastoreExistsException,
    DatastorePathExistsException,
    DatastoreSetupException,
)
from datastore_api.config import environment
from datastore_api.domain.datastores.models import NewDatastore

DATASTORES_ROOT_DIR = environment.datastores_root_dir

logger = logging.getLogger()


def get_datastore_dir_from_rdn(rdn: str) -> str:
    root_dir = Path(DATASTORES_ROOT_DIR)
    datastore_dir = root_dir / rdn
    if not datastore_dir.resolve().is_relative_to(root_dir.resolve()):
        raise ValueError(
            "Resolved datastore directory is outside allowed base directory"
        )
    return str(datastore_dir)


def create_new_datastore(
    new_datastore: NewDatastore,
    db_client: db.DatabaseClient,
    user_info: UserInfo,
) -> NewJobResponse:
    if new_datastore.rdn in db_client.get_datastores():
        raise DatastoreExistsException(
            f"Datastore rdn {new_datastore.rdn} already exists"
        )
    if Path(new_datastore.directory).exists():
        logger.error(
            f"Datastore directory exists ({new_datastore.directory}) "
            f"without matching rdn ({new_datastore.rdn}) in the database."
        )
        raise DatastorePathExistsException(
            f"Datastore directory already exists at {new_datastore.directory}"
        )
    db_client.insert_new_datastore(
        rdn=new_datastore.rdn,
        description=new_datastore.description,
        directory=new_datastore.directory,
        name=new_datastore.name,
        bump_enabled=new_datastore.bump_enabled,
    )
    try:
        setup_datastore(
            rdn=new_datastore.rdn,
            description=new_datastore.description,
            directory=new_datastore.directory,
            name=new_datastore.name,
        )
        job_params = JobParameters(
            target="DATASTORE", operation=Operation.GENERATE_RSA_KEYS
        )
        job = db_client.insert_new_job(
            Job(
                job_id="",
                status=JobStatus("queued"),
                parameters=job_params,
                created_at=datetime.now().isoformat(),
                created_by=user_info,
                datastore_rdn=new_datastore.rdn,
            )
        )
        return NewJobResponse(
            status="queued",
            msg="CREATED",
            job_id=str(job.job_id),
        )
    except Exception as e:
        db_client.hard_delete_datastore(rdn=new_datastore.rdn)
        raise DatastoreSetupException(
            f"Failed to set up datastore {new_datastore.rdn}"
        ) from e
