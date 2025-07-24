import logging

from fastapi import APIRouter

from datastore_api.adapter.local_storage import input_directory
from datastore_api.adapter.local_storage.input_directory import (
    ImportableDataset,
)

logger = logging.getLogger()

router = APIRouter()


@router.get("/")
def get_importable_datasets() -> list[ImportableDataset]:
    return input_directory.get_importable_datasets()


@router.delete("/{dataset_name}")
def delete_importable_datasets(dataset_name: str) -> dict:
    input_directory.delete_importable_datasets(dataset_name)
    return {"message": f"OK, {dataset_name} deleted"}
