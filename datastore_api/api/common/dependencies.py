import logging
from pathlib import Path

from fastapi import Depends, Request

from datastore_api.adapter import db
from datastore_api.common.exceptions import (
    DatastoreNotFoundException,
    DatastoreRdnMissingException,
)

logger = logging.getLogger()


def get_datastore_rdn_from_request(request: Request) -> str:
    datastore_rdn: str | None = request.path_params.get("datastore_rdn")

    if datastore_rdn is None:
        raise DatastoreRdnMissingException(
            "RDN is required but not found in request path"
        )

    return datastore_rdn


def get_datastore_id(
    request: Request,
    database_client: db.DatabaseClient = Depends(db.get_database_client),
) -> int:
    """Returns the datastore ID corresponding to the RDN in the request path.
    If no RDN is present (legacy endpoint), defaults to datastore_id=1."""

    datastore_rdn = get_datastore_rdn_from_request(request)
    datastore_id = database_client.get_datastore_id_from_rdn(datastore_rdn)

    if datastore_id is None:
        raise DatastoreNotFoundException(
            f"No datastore found for datastore_rdn: {datastore_rdn}"
        )
    return datastore_id


def get_datastore_root_dir(
    database_client: db.DatabaseClient = Depends(db.get_database_client),
    datastore_id: int = Depends(get_datastore_id),
) -> Path:
    """Returns the path to the datastore directory"""
    return Path(database_client.get_datastore(datastore_id).directory)
