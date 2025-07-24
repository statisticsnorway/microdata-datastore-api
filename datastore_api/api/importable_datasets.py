import logging

from fastapi import APIRouter

from datastore_api.adapter.local_storage import input_directory

logger = logging.getLogger()

router = APIRouter()


@router.get("/importable-datasets")
def get_importable_datasets():
    datasets = input_directory.get_importable_datasets()
    return [
        dataset.model_dump(exclude_none=True, by_alias=True)
        for dataset in datasets
    ]


@router.delete("/importable-datasets/{dataset_name}")
def delete_importable_datasets(dataset_name: str):
    input_directory.delete_importable_datasets(dataset_name)
    return {"message": f"OK, {dataset_name} deleted"}
