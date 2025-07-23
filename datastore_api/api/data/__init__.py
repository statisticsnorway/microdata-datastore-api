# pylint: disable=unused-argument
import logging

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import APIRouter, Depends, Header
from fastapi.responses import PlainTextResponse

from datastore_api.domain import data
from datastore_api.api.data.models import (
    ErrorMessage,
    InputTimePeriodQuery,
    InputTimeQuery,
    InputFixedQuery,
)
from datastore_api.adapter.auth import AuthClient, get_auth_client


data_router = APIRouter()
logger = logging.getLogger()


@data_router.post(
    "/data/event/stream", responses={404: {"model": ErrorMessage}}
)
def stream_result_event(
    input_query: InputTimePeriodQuery,
    authorization: str = Header(None),
    auth_client: AuthClient = Depends(get_auth_client),
):
    """
    Create Result set of data with temporality type event,
    and stream result as response.
    """
    logger.info(f"Entering /data/event/stream with input query: {input_query}")
    auth_client.authorize_user(authorization)
    result_data = data.process_event_request(input_query)
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(buffer_stream.getvalue().to_pybytes())


@data_router.post(
    "/data/status/stream", responses={404: {"model": ErrorMessage}}
)
def stream_result_status(
    input_query: InputTimeQuery,
    authorization: str = Header(None),
    auth_client: AuthClient = Depends(get_auth_client),
):
    """
    Create result set of data with temporality type status,
    and stream result as response.
    """
    logger.info(f"Entering /data/status/stream with input query: {input_query}")
    auth_client.authorize_user(authorization)
    result_data = data.process_status_request(input_query)
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(buffer_stream.getvalue().to_pybytes())


@data_router.post(
    "/data/fixed/stream", responses={404: {"model": ErrorMessage}}
)
def stream_result_fixed(
    input_query: InputFixedQuery,
    authorization: str = Header(None),
    auth_client: AuthClient = Depends(get_auth_client),
):
    """
    Create result set of data with temporality type fixed,
    and stream result as response.
    """
    logger.info(f"Entering /data/fixed/stream with input query: {input_query}")
    auth_client.authorize_user(authorization)
    result_data = data.process_fixed_request(input_query)
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(buffer_stream.getvalue().to_pybytes())
