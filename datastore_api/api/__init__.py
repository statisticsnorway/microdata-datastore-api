import logging
from typing import Awaitable, Callable

from fastapi import FastAPI, Request, Response, status
from fastapi.encoders import jsonable_encoder
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
            return JSONResponse(content={"message": str(exc)}, status_code=404)
        except RequestValidationException as exc:
            logger.warning(exc, exc_info=True)
            return JSONResponse(content={"message": str(exc)}, status_code=400)
        except InvalidStorageFormatException as exc:
            logger.exception(exc)
            return JSONResponse(
                content={"message": "Invalid storage format"}, status_code=500
            )
        except (
            ValidationError,
            ValueError,
            JobExistsException,
            NameValidationError,
            PublicKeyInvalidException,
        ) as exc:
            logger.warning(exc, exc_info=True)
            return JSONResponse(status_code=400, content={"message": str(exc)})
        except (
            DatastoreNotFoundException,
            DatastoreRdnMissingException,
            PublicKeyNotFoundException,
            NotFoundException,
        ) as exc:
            logger.warning(exc, exc_info=True)
            return JSONResponse(status_code=404, content={"message": str(exc)})
        except AuthError as exc:
            logger.warning(exc, exc_info=True)
            return JSONResponse(
                status_code=401, content={"message": "Unauthorized"}
            )
        except (
            DatastoreExistsException,
            DatastorePathExistsException,
            PublicKeyAlreadyExistsException,
        ) as exc:
            logger.warning(exc, exc_info=True)
            return JSONResponse(status_code=409, content={"message": str(exc)})
        except DatastoreSetupException as exc:
            logger.error(exc, exc_info=True)
            return JSONResponse(status_code=500, content={"message": str(exc)})

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
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=jsonable_encoder(
                {"message": "Bad Request", "details": exc.errors()}
            ),
        )

    @app.exception_handler(HTTPException)
    def handle_http_exception(
        _req: Request, exc: HTTPException
    ) -> JSONResponse:
        if exc.status_code == 404:
            logger.warning(exc, exc_info=True)
            return JSONResponse(
                content={"message": "Not found"}, status_code=404
            )
        logger.exception(exc)
        return JSONResponse(
            status_code=500,
            content={"message": "Internal Server Error"},
        )

    @app.exception_handler(Exception)
    def handle_generic_exception(_req: Request, exc: Exception) -> JSONResponse:
        logger.exception(exc, exc_info=True)
        return JSONResponse(
            status_code=500,
            content=jsonable_encoder({"message": "Internal Server Error"}),
        )
