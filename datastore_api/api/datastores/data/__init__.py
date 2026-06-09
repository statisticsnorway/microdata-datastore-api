# pylint: disable=unused-argument
import logging
from typing import Annotated

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import APIRouter, Depends
from fastapi.responses import PlainTextResponse
from pyarrow import dataset

from datastore_api.adapter.auth.dependencies import authorize_user
from datastore_api.api.common.dependencies import (
    get_data_reader,
)
from datastore_api.domain.data import (
    DataReader,
    generate_data_filter,
)
from datastore_api.domain.data.models import (
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
    data_filter: Annotated[dataset.Expression, Depends(generate_data_filter)],
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
    data_filter: Annotated[dataset.Expression, Depends(generate_data_filter)],
) -> PlainTextResponse:
    """
    Create result set of data with temporality type status,
    and stream result as response.
    """
    logger.info(f"Entering /data/status/stream with input query: {input_query}")
    result_data = data_reader.read_data(data_filter)
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
    data_filter: Annotated[dataset.Expression, Depends(generate_data_filter)],
) -> PlainTextResponse:
    """
    Create result set of data with temporality type fixed,
    and stream result as response.
    """
    logger.info(f"Entering /data/fixed/stream with input query: {input_query}")
    result_data = data_reader.read_data(data_filter)
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(buffer_stream.getvalue().to_pybytes())
