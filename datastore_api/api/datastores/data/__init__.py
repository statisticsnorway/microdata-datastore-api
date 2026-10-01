# pylint: disable=unused-argument
import logging
from typing import Annotated

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse
from pyarrow import dataset

from datastore_api.adapter import db
from datastore_api.adapter.auth.dependencies import (
    authorize_api_key,
    authorize_user,
)
from datastore_api.api.common.dependencies import (
    get_data_reader,
    get_datastore_id,
    get_datastore_root_dir,
)
from datastore_api.common.models import Version
from datastore_api.config import environment
from datastore_api.domain.data import (
    DataReader,
    generate_fixed_filter,
    generate_time_filter,
    generate_time_period_filter,
    validate_encryption,
)
from datastore_api.domain.data.models import (
    EncryptionStatus,
    ErrorMessage,
    InputFixedQuery,
    InputTimePeriodQuery,
    InputTimeQuery,
)

router = APIRouter()
logger = logging.getLogger()


@router.post(
    "/event/stream",
    responses={404: {"model": ErrorMessage}},
    dependencies=[Depends(authorize_user)],
)
def stream_result_event(
    input_query: InputTimePeriodQuery,
    data_reader: Annotated[DataReader, Depends(get_data_reader)],
    data_filter: Annotated[
        dataset.Expression, Depends(generate_time_period_filter)
    ],
) -> PlainTextResponse:
    """
    Create Result set of data with temporality type event,
    and stream result as response.
    """
    logger.info(f"Entering /data/event/stream with input query: {input_query}")
    result_data = data_reader.read_data(data_filter)
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(buffer_stream.getvalue().to_pybytes())


@router.post(
    "/status/stream",
    responses={404: {"model": ErrorMessage}},
    dependencies=[Depends(authorize_user)],
)
def stream_result_status(
    input_query: InputTimeQuery,
    data_reader: Annotated[DataReader, Depends(get_data_reader)],
    data_filter: Annotated[dataset.Expression, Depends(generate_time_filter)],
) -> PlainTextResponse:
    """
    Create result set of data with temporality type status,
    and stream result as response.
    """
    logger.info(f"Entering /data/status/stream with input query: {input_query}")
    result_data = data_reader.read_data(
        data_filter, row_cap=environment.data_row_cap
    )
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(buffer_stream.getvalue().to_pybytes())


@router.post(
    "/fixed/stream",
    responses={404: {"model": ErrorMessage}},
    dependencies=[Depends(authorize_user)],
)
def stream_result_fixed(
    input_query: InputFixedQuery,
    data_reader: Annotated[DataReader, Depends(get_data_reader)],
    data_filter: Annotated[
        dataset.Expression | None, Depends(generate_fixed_filter)
    ],
) -> PlainTextResponse:
    """
    Create result set of data with temporality type fixed,
    and stream result as response.
    """
    logger.info(f"Entering /data/fixed/stream with input query: {input_query}")
    result_data = data_reader.read_data(
        data_filter, row_cap=environment.data_row_cap
    )
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(buffer_stream.getvalue().to_pybytes())


@router.get(
    "/encryption-status",
    dependencies=[Depends(authorize_api_key)],
)
async def encryption_status(
    version: str = Query(...),
    database_client: db.DatabaseClient = Depends(db.get_database_client),
    datastore_id: int = Depends(get_datastore_id),
) -> EncryptionStatus:
    parsed_version = Version.from_str(version)
    root_dir = get_datastore_root_dir(database_client, datastore_id)
    return validate_encryption(root_dir, parsed_version)
