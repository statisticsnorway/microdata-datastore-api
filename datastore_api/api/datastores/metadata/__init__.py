from pathlib import Path

from fastapi import APIRouter, Depends

from datastore_api.api.common.dependencies import get_datastore_root_dir
from datastore_api.api.datastores.metadata.models import (
    MetadataQuery,
    NameParam,
    get_metadata_query,
)
from datastore_api.api.error_contract import (
    DATA_NOT_FOUND,
    INVALID_DRAFT_VERSION,
    REQUEST_PARSE_ERROR,
    REQUEST_VALIDATION_ERROR,
    SYSTEM_ERROR,
    document_error_responses,
)
from datastore_api.domain import metadata

router = APIRouter(
    responses=document_error_responses(
        REQUEST_VALIDATION_ERROR, DATA_NOT_FOUND, SYSTEM_ERROR
    )
)


@router.get("/data-store")
def get_data_store(
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
) -> dict:
    return metadata.find_all_datastore_versions(datastore_root_dir)


@router.get("/data-structures/status")
def get_data_structure_current_status(
    query: NameParam = Depends(),
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
) -> dict:
    return metadata.find_current_data_structure_status(
        query.get_names_as_list(), datastore_root_dir
    )


@router.post(
    "/data-structures/status",
    responses=document_error_responses(
        REQUEST_VALIDATION_ERROR, REQUEST_PARSE_ERROR
    ),
)
def get_data_structure_current_status_as_post(
    body: NameParam, datastore_root_dir: Path = Depends(get_datastore_root_dir)
) -> dict[str, dict | None]:
    return metadata.find_current_data_structure_status(
        body.get_names_as_list(), datastore_root_dir
    )


@router.get(
    "/data-structures",
    responses=document_error_responses(
        REQUEST_VALIDATION_ERROR, INVALID_DRAFT_VERSION
    ),
)
def get_data_structures(
    query: MetadataQuery = Depends(get_metadata_query),
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
) -> list[dict]:
    return metadata.find_data_structures(
        datastore_root_dir,
        query.names_as_list(),
        query.version,
        query.include_attributes,
        query.skip_code_lists,
    )


@router.get("/all-data-structures")
def get_all_data_structures_ever(
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
) -> list[str]:
    return metadata.find_all_data_structures_ever(datastore_root_dir)


@router.get(
    "/all",
    responses=document_error_responses(
        REQUEST_VALIDATION_ERROR, INVALID_DRAFT_VERSION
    ),
)
def get_all_metadata(
    query: MetadataQuery = Depends(get_metadata_query),
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
) -> dict:
    return metadata.find_all_metadata(
        query.version, datastore_root_dir, query.skip_code_lists
    )
