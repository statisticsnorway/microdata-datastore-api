from pathlib import Path

from fastapi import APIRouter, Depends

from datastore_api.adapter import db
from datastore_api.api.metadata.models import (
    MetadataQuery,
    NameParam,
    get_metadata_query,
)
from datastore_api.domain import metadata

router = APIRouter()


@router.get("/data-store")
def get_data_store(
    database_client: db.DatabaseClient = Depends(db.get_database_client),
) -> dict:
    datastore_root_dir = Path(database_client.get_datastore().directory)
    return metadata.find_all_datastore_versions(datastore_root_dir)


@router.get("/data-structures/status")
def get_data_structure_current_status(
    query: NameParam = Depends(),
    database_client: db.DatabaseClient = Depends(db.get_database_client),
) -> dict:
    datastore_root_dir = Path(database_client.get_datastore().directory)
    return metadata.find_current_data_structure_status(
        query.get_names_as_list(), datastore_root_dir
    )


@router.post("/data-structures/status")
def get_data_structure_current_status_as_post(
    body: NameParam,
    database_client: db.DatabaseClient = Depends(db.get_database_client),
) -> dict[str, dict | None]:
    datastore_root_dir = Path(database_client.get_datastore().directory)
    return metadata.find_current_data_structure_status(
        body.get_names_as_list(), datastore_root_dir
    )


@router.get("/data-structures")
def get_data_structures(
    query: MetadataQuery = Depends(get_metadata_query),
    database_client: db.DatabaseClient = Depends(db.get_database_client),
) -> list[dict]:
    query.include_attributes = True
    datastore_root_dir = Path(database_client.get_datastore().directory)
    return metadata.find_data_structures(
        datastore_root_dir,
        query.names_as_list(),
        query.version,
        query.include_attributes,
        query.skip_code_lists,
    )


@router.get("/all-data-structures")
def get_all_data_structures_ever(
    database_client: db.DatabaseClient = Depends(db.get_database_client),
) -> list[str]:
    datastore_root_dir = Path(database_client.get_datastore().directory)
    return metadata.find_all_data_structures_ever(datastore_root_dir)


@router.get("/all")
def get_all_metadata(
    query: MetadataQuery = Depends(get_metadata_query),
    database_client: db.DatabaseClient = Depends(db.get_database_client),
) -> dict:
    datastore_root_dir = Path(database_client.get_datastore().directory)
    return metadata.find_all_metadata(
        query.version, datastore_root_dir, query.skip_code_lists
    )
