from fastapi import APIRouter, Depends

from datastore_api.api.metadata.models import (
    MetadataQuery,
    NameParam,
    get_metadata_query,
)
from datastore_api.domain import metadata

router = APIRouter()


@router.get("/data-store")
def get_data_store() -> dict:
    return metadata.find_all_datastore_versions()


@router.get("/data-structures/status")
def get_data_structure_current_status(query: NameParam = Depends()) -> dict:
    return metadata.find_current_data_structure_status(
        query.get_names_as_list()
    )


@router.post("/data-structures/status")
def get_data_structure_current_status_as_post(
    body: NameParam,
) -> dict[str, dict | None]:
    return metadata.find_current_data_structure_status(body.get_names_as_list())


@router.get("/data-structures")
def get_data_structures(
    query: MetadataQuery = Depends(get_metadata_query),
) -> list[dict]:
    query.include_attributes = True
    return metadata.find_data_structures(
        query.names_as_list(),
        query.version,
        query.include_attributes,
        query.skip_code_lists,
    )


@router.get("/all-data-structures")
def get_all_data_structures_ever() -> list[str]:
    return metadata.find_all_data_structures_ever()


@router.get("/all")
def get_all_metadata(
    query: MetadataQuery = Depends(get_metadata_query),
) -> dict:
    return metadata.find_all_metadata(
        query.version,
        query.skip_code_lists,
    )
