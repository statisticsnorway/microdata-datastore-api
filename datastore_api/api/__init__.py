import logging
from typing import Awaitable, Callable

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.exceptions import HTTPException

from datastore_api.api import (
    datastores,
    jobs,
    maintenance_statuses,
    observability,
)
from datastore_api.api.error_contract import (
    AUTHENTICATION_FAILURE,
    DATA_NOT_FOUND,
    INVALID_DRAFT_VERSION,
    PATH_NOT_FOUND,
    REQUEST_PARSE_ERROR,
    REQUEST_VALIDATION_ERROR,
    ROW_LIMIT_EXCEEDED,
    SYSTEM_ERROR,
    contract_error_response,
    message_response,
)
from datastore_api.common.exceptions import (
    AuthError,
    DatastoreExistsException,
    DatastoreNotFoundException,
    DatastorePathExistsException,
    DatastoreRdnMissingException,
    DatastoreSetupException,
    InvalidDraftVersionException,
    InvalidStorageFormatException,
    JobExistsException,
    NameValidationError,
    NotFoundException,
    PublicKeyAlreadyExistsException,
    PublicKeyInvalidException,
    PublicKeyNotFoundException,
    RequestValidationException,
    TooManyRowsException,
)

logger = logging.getLogger()


def setup_api(app: FastAPI) -> None:
    _include_middleware(app)
    _include_exception_handlers(app)
    _include_routers(app)


def _include_routers(app: FastAPI) -> None:
    app.include_router(observability.router, prefix="/health")
    app.include_router(datastores.router, prefix="/datastores")
    app.include_router(
        maintenance_statuses.router, prefix="/maintenance-statuses"
    )
    app.include_router(jobs.router, prefix="/jobs")


def _include_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def handle_exceptions(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """
        Handle all exceptions coming from the application and make sure they
        return an appropriate response. If any exceptions are not caught here
        they will raise to the application exception handlers definted below.
        """
        try:
            return await call_next(request)
        except InvalidDraftVersionException as exc:
            logger.warning(exc, exc_info=True)
            return contract_error_response(
                INVALID_DRAFT_VERSION,
                data={
                    "requested_draft_version": exc.requested_draft_version,
                    "current_draft_version": exc.current_draft_version,
                },
            )
        except TooManyRowsException as exc:
            logger.warning(exc, exc_info=True)
            return contract_error_response(
                ROW_LIMIT_EXCEEDED,
                data={
                    "row_limit": exc.row_limit,
                    "row_count": exc.row_count,
                },
            )
        except RequestValidationException as exc:
            logger.warning(exc, exc_info=True)
            return contract_error_response(REQUEST_VALIDATION_ERROR)
        except InvalidStorageFormatException as exc:
            logger.exception(exc)
            return contract_error_response(SYSTEM_ERROR)
        except (
            ValidationError,
            ValueError,
            JobExistsException,
            NameValidationError,
            PublicKeyInvalidException,
        ) as exc:
            logger.warning(exc, exc_info=True)
            return message_response(400, str(exc))
        except (
            DatastoreNotFoundException,
            DatastoreRdnMissingException,
            PublicKeyNotFoundException,
            NotFoundException,
        ) as exc:
            logger.warning(exc, exc_info=True)
            return contract_error_response(DATA_NOT_FOUND)
        except AuthError as exc:
            logger.warning(exc, exc_info=True)
            return contract_error_response(AUTHENTICATION_FAILURE)
        except (
            DatastoreExistsException,
            DatastorePathExistsException,
            PublicKeyAlreadyExistsException,
        ) as exc:
            logger.warning(exc, exc_info=True)
            return message_response(409, str(exc))
        except DatastoreSetupException as exc:
            logger.error(exc, exc_info=True)
            return contract_error_response(SYSTEM_ERROR)

    @app.middleware("http")
    async def add_language_header(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        response = await call_next(request)
        if request.url.path.startswith("/metadata"):
            response.headers.setdefault("Content-Language", "no")
        return response


def _include_exception_handlers(app: FastAPI) -> None:
    """
    Exception handlers to handle any exceptions that have raised to the top of
    the application.
    FastAPI initial pydantic validations are also done before hitting any
    middleware so they are dealt with here.
    """

    @app.exception_handler(RequestValidationError)
    def handle_pydantic_query_error(
        _req: Request, exc: RequestValidationError
    ) -> JSONResponse:
        logger.exception(exc)
        errors = exc.errors()
        if any(error.get("type") == "json_invalid" for error in errors):
            return contract_error_response(REQUEST_PARSE_ERROR)
        return contract_error_response(
            REQUEST_VALIDATION_ERROR, data={"errors": errors}
        )

    @app.exception_handler(HTTPException)
    def handle_http_exception(
        _req: Request, exc: HTTPException
    ) -> JSONResponse:
        if exc.status_code == 400:
            logger.warning(exc, exc_info=True)
            return contract_error_response(REQUEST_VALIDATION_ERROR)
        if exc.status_code == 404:
            logger.warning(exc, exc_info=True)
            return contract_error_response(PATH_NOT_FOUND)
        logger.exception(exc)
        return contract_error_response(SYSTEM_ERROR)

    @app.exception_handler(Exception)
    def handle_generic_exception(_req: Request, exc: Exception) -> JSONResponse:
        logger.exception(exc, exc_info=True)
        return contract_error_response(SYSTEM_ERROR)
