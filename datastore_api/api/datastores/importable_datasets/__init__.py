import logging
from pathlib import Path

from fastapi import APIRouter, Depends

from datastore_api.adapter import db
from datastore_api.adapter.auth.dependencies import authorize_data_administrator
from datastore_api.adapter.db import DatabaseClient
from datastore_api.adapter.local_storage import input_directory
from datastore_api.api.common.dependencies import (
    get_datastore_id,
    get_datastore_root_dir,
)
from datastore_api.domain import importable_datasets
from datastore_api.domain.importable_datasets import ImportableModel

logger = logging.getLogger()

router = APIRouter()


@router.get("", dependencies=[Depends(authorize_data_administrator)])
def get_importable_datasets(
    db_client: DatabaseClient = Depends(db.get_database_client),
    datastore_id: int = Depends(get_datastore_id),
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
) -> list[ImportableModel]:
    in_progress_targets = [
        job.parameters.target
        for job in db_client.get_jobs(
            datastore_id=datastore_id,
            status=None,
            operations=None,
            ignore_completed=True,
        )
    ]
    datastore_input_dir = Path(str(datastore_root_dir) + "_input")
    return importable_datasets.find_importables(
        datastore_input_dir=datastore_input_dir,
        datastore_root_dir=datastore_root_dir,
        filter_out=in_progress_targets,
    )


@router.delete(
    "/{dataset_name}", dependencies=[Depends(authorize_data_administrator)]
)
def delete_importable_datasets(
    dataset_name: str,
    db_client: DatabaseClient = Depends(db.get_database_client),
    datastore_id: int = Depends(get_datastore_id),
) -> dict:
    datastore_input_dir = Path(
        db_client.get_datastore(datastore_id).directory + "_input"
    )
    input_directory.delete_importable_datasets(
        dataset_name, datastore_input_dir
    )
    return {"message": f"OK, {dataset_name} deleted"}
