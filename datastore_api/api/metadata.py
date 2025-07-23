from fastapi import APIRouter, Depends

from datastore_api.api.request_models import NameParam, MetadataQuery
from datastore_api.domain import metadata
from datastore_api.common.models import Version

metadata_router = APIRouter()


@metadata_router.get("/metadata/data-store")
def get_data_store():
    return metadata.find_all_datastore_versions()


@metadata_router.get("/metadata/data-structures/status")
def get_data_structure_current_status(validated_query: NameParam = Depends()):
    return metadata.find_current_data_structure_status(
        validated_query.get_names_as_list()
    )


@metadata_router.post("/metadata/data-structures/status")
def get_data_structure_current_status_as_post(validated_body: NameParam):
    return metadata.find_current_data_structure_status(
        validated_body.get_names_as_list()
    )


@metadata_router.get("/metadata/data-structures")
def get_data_structures(
    validated_query: MetadataQuery = Depends(),
):
    validated_query.include_attributes = True
    return metadata.find_data_structures(
        validated_query.names_as_list(),
        Version.from_str(validated_query.version),
        validated_query.include_attributes,
        validated_query.skip_code_lists,
    )


@metadata_router.get("/metadata/all-data-structures")
def get_all_data_structures_ever():
    return metadata.find_all_data_structures_ever()


@metadata_router.get("/metadata/all")
def get_all_metadata(
    validated_query: MetadataQuery = Depends(),
):
    return metadata.find_all_metadata(
        Version.from_str(validated_query.version),
        validated_query.skip_code_lists,
    )


@metadata_router.get("/languages")
def get_languages():
    return metadata.find_languages()
