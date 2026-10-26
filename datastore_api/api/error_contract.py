"""Shared API error responses and consumer contract definitions."""

from dataclasses import dataclass

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from datastore_api.domain.data.models import ErrorMessage


@dataclass(frozen=True)
class ErrorDefinition:
    type: str
    code: int
    http_status: int
    message: str


REQUEST_VALIDATION_ERROR = ErrorDefinition(
    type="REQUEST_VALIDATION_ERROR",
    code=101,
    http_status=400,
    message="The request payload does not match the required format.",
)

REQUEST_PARSE_ERROR = ErrorDefinition(
    type="REQUEST_PARSE_ERROR",
    code=102,
    http_status=400,
    message="An error occurred when parsing the request payload.",
)

PATH_NOT_FOUND = ErrorDefinition(
    type="PATH_NOT_FOUND",
    code=103,
    http_status=404,
    message="The url in the request points to a path that don't exist.",
)

AUTHENTICATION_FAILURE = ErrorDefinition(
    type="AUTHENTICATION_FAILURE",
    code=104,
    http_status=401,
    message="The provided JWT was not accepted",
)

DATA_NOT_FOUND = ErrorDefinition(
    type="DATA_NOT_FOUND",
    code=105,
    http_status=404,
    message="No data exists for this payload.",
)

ROW_LIMIT_EXCEEDED = ErrorDefinition(
    type="ROW_LIMIT_EXCEEDED",
    code=106,
    http_status=400,
    message=(
        "This import would produce a dataset containing more rows "
        "than the row_limit."
    ),
)

INVALID_DRAFT_VERSION = ErrorDefinition(
    type="INVALID_DRAFT_VERSION",
    code=107,
    http_status=400,
    message="Requested draft version does not match the current draft.",
)

SYSTEM_ERROR = ErrorDefinition(
    type="SYSTEM_ERROR",
    code=202,
    http_status=500,
    message="An unexpected system error occured.",
)


def contract_error_response(
    error: ErrorDefinition,
    *,
    data: dict | None = None,
) -> JSONResponse:
    content = {
        "type": error.type,
        "code": error.code,
        "message": error.message,
    }
    if data is not None:
        content["data"] = data
    return JSONResponse(
        status_code=error.http_status, content=jsonable_encoder(content)
    )


def message_response(status_code: int, message: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"message": message})


def document_error_responses(*errors: ErrorDefinition) -> dict:
    """Document applicable errors, grouped by their HTTP status."""
    responses = {}
    for error in errors:
        response = responses.setdefault(
            error.http_status,
            {
                "model": ErrorMessage,
                "description": "",
                "content": {"application/json": {"examples": {}}},
            },
        )
        example = {
            "type": error.type,
            "code": error.code,
            "message": error.message,
        }
        response["content"]["application/json"]["examples"][error.type] = {
            "value": example
        }
        if response["description"]:
            response["description"] += "\n\n"
        response["description"] += (
            f"{error.type} ({error.code}): {error.message}"
        )
        if error == ROW_LIMIT_EXCEEDED:
            example["data"] = {"row_limit": 20000, "row_count": 20100}
            response["description"] += (
                " The data object contains row_limit (configured maximum) "
                "and row_count (actual result size)"
            )
        elif error == INVALID_DRAFT_VERSION:
            example["data"] = {
                "requested_draft_version": "0.0.0.1608000000",
                "current_draft_version": "0.0.0.1609000000",
            }
            response["description"] += (
                " The data object contains requested_draft_version and "
                "current_draft_version."
            )
    return responses
