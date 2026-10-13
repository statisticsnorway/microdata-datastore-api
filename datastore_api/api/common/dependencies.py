import logging
from pathlib import Path

from fastapi import Depends

from datastore_api.adapter import db
from datastore_api.adapter.local_storage.datastore_directory import (
    validate_path,
)
from datastore_api.domain.data import DataReader, select_data_reader
from datastore_api.domain.data.models import (
    InputFixedQuery,
    InputTimePeriodQuery,
    InputTimeQuery,
)

logger = logging.getLogger()


def get_datastore_id(
    datastore_rdn: str,
    database_client: db.DatabaseClient = Depends(db.get_database_client),
) -> int:
    """Returns the datastore ID corresponding to the RDN in the request path"""
    return database_client.get_datastore_id_from_rdn(datastore_rdn)


def get_datastore_root_dir(
    database_client: db.DatabaseClient = Depends(db.get_database_client),
    datastore_id: int = Depends(get_datastore_id),
) -> Path:
    """Returns the path to the datastore directory"""
    path = Path(database_client.get_datastore(datastore_id).directory)
    resolved_path = validate_path(path)
    return resolved_path


def get_data_reader(
    input_query: InputTimePeriodQuery | InputTimeQuery | InputFixedQuery,
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
) -> DataReader:
    return select_data_reader(input_query, datastore_root_dir)
