# pylint: disable=unused-argument
import logging
from typing import Annotated

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import APIRouter, Depends
from fastapi.responses import PlainTextResponse
from pyarrow import dataset

from datastore_api.adapter.auth.dependencies import (
    authorize_api_key,
    authorize_user,
)
from datastore_api.api.common.dependencies import get_data_reader
from datastore_api.api.error_contract import (
    AUTHENTICATION_FAILURE,
    DATA_NOT_FOUND,
    REQUEST_PARSE_ERROR,
    REQUEST_VALIDATION_ERROR,
    ROW_LIMIT_EXCEEDED,
    SYSTEM_ERROR,
    document_error_responses,
)
from datastore_api.common.exceptions import UnexpectedDataReaderException
from datastore_api.config import environment
from datastore_api.domain.data import (
    DataReader,
    EncryptedDataReader,
    generate_fixed_filter,
    generate_time_filter,
    generate_time_period_filter,
)
from datastore_api.domain.data.models import (
    InputFixedQuery,
    InputTimePeriodQuery,
    InputTimeQuery,
)

router = APIRouter(
    responses=document_error_responses(
        REQUEST_VALIDATION_ERROR,
        REQUEST_PARSE_ERROR,
        AUTHENTICATION_FAILURE,
        DATA_NOT_FOUND,
        SYSTEM_ERROR,
    )
)
logger = logging.getLogger()


@router.post(
    "/event/stream",
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
    responses=document_error_responses(
        REQUEST_VALIDATION_ERROR, REQUEST_PARSE_ERROR, ROW_LIMIT_EXCEEDED
    ),
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
    responses=document_error_responses(
        REQUEST_VALIDATION_ERROR, REQUEST_PARSE_ERROR, ROW_LIMIT_EXCEEDED
    ),
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


@router.post(
    "/encryption-status",
    dependencies=[Depends(authorize_api_key)],
)
def encryption_status(
    input_query: InputFixedQuery,
    data_reader: Annotated[DataReader, Depends(get_data_reader)],
    data_filter: Annotated[
        dataset.Expression | None, Depends(generate_fixed_filter)
    ],
) -> PlainTextResponse:
    """
    Assert that a dataset is encrypted through the data reader type, then
    attempt to read one row of the dataset using the data reader and stream
    result as response.
    """
    logger.info(
        f"Entering /data/encryption-status with input query: {input_query}"
    )
    if not isinstance(data_reader, EncryptedDataReader):
        raise UnexpectedDataReaderException(
            "Non-encrypted data reader detected."
        )
    result_data = data_reader.read_data(
        data_filter,
        row_cap=environment.data_row_cap,
        reader_test=True,
    )
    buffer_stream = pa.BufferOutputStream()
    pq.write_table(result_data, buffer_stream)
    return PlainTextResponse(buffer_stream.getvalue().to_pybytes())
