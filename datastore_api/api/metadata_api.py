import logging

from fastapi import APIRouter, Depends

from datastore_api.api.request_models import NameParam, MetadataQuery
from datastore_api.domain import metadata
from datastore_api.domain.version import get_version_from_string

logger = logging.getLogger()
metadata_router = APIRouter()


@metadata_router.get("/metadata/data-store")
def get_data_store():
    logger.info("GET /metadata/data-store")
    return metadata.find_all_datastore_versions()


@metadata_router.get("/metadata/data-structures/status")
def get_data_structure_current_status(validated_query: NameParam = Depends()):
    logger.info(
        f"GET /metadata/data-structures/status with name = {validated_query.names}"
    )
    return metadata.find_current_data_structure_status(
        validated_query.get_names_as_list()
    )


@metadata_router.post("/metadata/data-structures/status")
def get_data_structure_current_status_as_post(validated_body: NameParam):
    logger.info(
        f"POST /metadata/data-structures/status with name = {validated_body.names}"
    )
    return metadata.find_current_data_structure_status(
        validated_body.get_names_as_list()
    )


@metadata_router.get("/metadata/data-structures")
def get_data_structures(
    validated_query: MetadataQuery = Depends(),
):
    validated_query.include_attributes = True
    logger.info(f"GET /metadata/data-structures with query: {validated_query}")
    return metadata.find_data_structures(
        validated_query.names_as_list(),
        get_version_from_string(validated_query.version),
        validated_query.include_attributes,
        validated_query.skip_code_lists,
    )


@metadata_router.get("/metadata/all-data-structures")
def get_all_data_structures_ever():
    logger.info("GET /metadata/all-data-structures")
    return metadata.find_all_data_structures_ever()


@metadata_router.get("/metadata/all")
def get_all_metadata(
    validated_query: MetadataQuery = Depends(),
):
    logger.info(f"GET /metadata/all with version: {validated_query.version}")
    return metadata.find_all_metadata(
        get_version_from_string(validated_query.version),
        validated_query.skip_code_lists,
    )


@metadata_router.get("/languages")
def get_languages():
    logger.info("GET /languages")
    return metadata.find_languages()
