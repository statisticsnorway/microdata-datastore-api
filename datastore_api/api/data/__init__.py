# pylint: disable=unused-argument
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import APIRouter, Depends, Header
from fastapi.responses import PlainTextResponse

from datastore_api.adapter.auth import AuthClient, get_auth_client
from datastore_api.api.data.models import (
    ErrorMessage,
    InputFixedQuery,
    InputTimePeriodQuery,
    InputTimeQuery,
)
from datastore_api.api.dependencies.datastore import get_datastore_root_dir
from datastore_api.domain import data

router = APIRouter()
logger = logging.getLogger()


@router.post("/event/stream", responses={404: {"model": ErrorMessage}})
def stream_result_event(
    input_query: InputTimePeriodQuery,
    authorization: str = Header(None),
    auth_client: AuthClient = Depends(get_auth_client),
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
) -> PlainTextResponse:
    """
    Create Result set of data with temporality type event,
    and stream result as response.
    """
    logger.info(f"Entering /data/event/stream with input query: {input_query}")
    auth_client.authorize_user(authorization)
    result_data = data.process_event_request(
        input_query.dataStructureName,
        input_query.version,
        input_query.population,
        input_query.includeAttributes,
        input_query.startDate,
        input_query.stopDate,
        datastore_root_dir,
    )
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(buffer_stream.getvalue().to_pybytes())


@router.post("/status/stream", responses={404: {"model": ErrorMessage}})
def stream_result_status(
    input_query: InputTimeQuery,
    authorization: str = Header(None),
    auth_client: AuthClient = Depends(get_auth_client),
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
) -> PlainTextResponse:
    """
    Create result set of data with temporality type status,
    and stream result as response.
    """
    logger.info(f"Entering /data/status/stream with input query: {input_query}")
    auth_client.authorize_user(authorization)
    result_data = data.process_status_request(
        input_query.dataStructureName,
        input_query.version,
        input_query.population,
        input_query.includeAttributes,
        input_query.date,
        datastore_root_dir,
    )
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(buffer_stream.getvalue().to_pybytes())


@router.post("/fixed/stream", responses={404: {"model": ErrorMessage}})
def stream_result_fixed(
    input_query: InputFixedQuery,
    authorization: str = Header(None),
    auth_client: AuthClient = Depends(get_auth_client),
    datastore_root_dir: Path = Depends(get_datastore_root_dir),
) -> PlainTextResponse:
    """
    Create result set of data with temporality type fixed,
    and stream result as response.
    """
    logger.info(f"Entering /data/fixed/stream with input query: {input_query}")
    auth_client.authorize_user(authorization)
    result_data = data.process_fixed_request(
        input_query.dataStructureName,
        input_query.version,
        input_query.population,
        input_query.includeAttributes,
        datastore_root_dir,
    )
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(buffer_stream.getvalue().to_pybytes())
