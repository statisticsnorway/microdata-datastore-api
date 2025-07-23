from fastapi import APIRouter, Depends

from datastore_api.api.metadata.models import (
    NameParam,
    MetadataQuery,
    get_metadata_query,
)
from datastore_api.domain import metadata

metadata_router = APIRouter()


@metadata_router.get("/metadata/data-store")
def get_data_store():
    return metadata.find_all_datastore_versions()


@metadata_router.get("/metadata/data-structures/status")
def get_data_structure_current_status(query: NameParam = Depends()):
    return metadata.find_current_data_structure_status(
        query.get_names_as_list()
    )


@metadata_router.post("/metadata/data-structures/status")
def get_data_structure_current_status_as_post(body: NameParam):
    return metadata.find_current_data_structure_status(body.get_names_as_list())


@metadata_router.get("/metadata/data-structures")
def get_data_structures(query: MetadataQuery = Depends(get_metadata_query)):
    query.include_attributes = True
    return metadata.find_data_structures(
        query.names_as_list(),
        query.version,
        query.include_attributes,
        query.skip_code_lists,
    )


@metadata_router.get("/metadata/all-data-structures")
def get_all_data_structures_ever():
    return metadata.find_all_data_structures_ever()


@metadata_router.get("/metadata/all")
def get_all_metadata(
    query: MetadataQuery = Depends(get_metadata_query),
):
    return metadata.find_all_metadata(
        query.version,
        query.skip_code_lists,
    )


@metadata_router.get("/languages")
def get_languages():
    return metadata.find_languages()
