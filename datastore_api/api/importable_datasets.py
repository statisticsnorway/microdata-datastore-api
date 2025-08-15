import logging

from fastapi import APIRouter, Depends

from datastore_api.adapter import db
from datastore_api.adapter.db import DatabaseClient
from datastore_api.adapter.local_storage import input_directory
from datastore_api.adapter.local_storage.input_directory import (
    ImportableDataset,
)

logger = logging.getLogger()

router = APIRouter()

"""
TODO:
This function should be improved further together with changes in the
frontend adapter code. We should move the "createImportable" function
to the backend to reduce redundant backend calls, and instead return
a frontend ready object like:
{
    dataset_name: str
    operation: "-" | "ADD" | "CHANGE" | "PATCH_METADATA"
    is_archived: bool
}
Choosing to only implement the in_progress_targets filtering here
as it is non-breaking.
"""


@router.get("")
def get_importable_datasets(
    db_client: DatabaseClient = Depends(db.get_database_client),
) -> list[ImportableDataset]:
    in_progress_targets = [
        job.parameters.target
        for job in db_client.get_jobs(
            status=None, operations=None, ignore_completed=True
        )
    ]
    return input_directory.get_importable_datasets(
        filter_out=in_progress_targets
    )


@router.delete("/{dataset_name}")
def delete_importable_datasets(dataset_name: str) -> dict:
    input_directory.delete_importable_datasets(dataset_name)
    return {"message": f"OK, {dataset_name} deleted"}
