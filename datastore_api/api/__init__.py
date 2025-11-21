import logging
from typing import Awaitable, Callable

from fastapi import FastAPI, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.exceptions import HTTPException

from datastore_api.api import (
    data,
    datastores,
    importable_datasets,
    jobs,
    languages,
    maintenance_statuses,
    metadata,
    observability,
    targets,
)
from datastore_api.common.exceptions import (
    AuthError,
    DatastoreNotFoundException,
    InvalidDraftVersionException,
    InvalidStorageFormatException,
    JobExistsException,
    NameValidationError,
    NotFoundException,
    RequestValidationException,
)

logger = logging.getLogger()


def setup_api(app: FastAPI) -> None:
    _include_exception_handlers(app)
    _include_middleware(app)
    _include_routers(app)


def _include_routers(app: FastAPI) -> None:
    app.include_router(datastores.router, prefix="/datastores")

    app.include_router(data.router, prefix="/datastores/{datastore_rdn}/data")
    app.include_router(
        metadata.router, prefix="/datastores/{datastore_rdn}/metadata"
    )
    app.include_router(observability.router, prefix="/health")
    app.include_router(
        observability.router, prefix="/datastores/{datastore_rdn}/health"
    )
    app.include_router(
        maintenance_statuses.router, prefix="/maintenance-statuses"
    )
    app.include_router(jobs.router, prefix="/jobs")
    app.include_router(
        languages.router, prefix="/datastores/{datastore_rdn}/languages"
    )
    app.include_router(
        targets.router, prefix="/datastores/{datastore_rdn}/targets"
    )
    app.include_router(
        importable_datasets.router,
        prefix="/datastores/{datastore_rdn}/importable-datasets",
    )

    # TODO: Remove legacy routers once all clients use datastore-specific URLs
    #       and move router definitions into the datastore/-api directory
    #       when applicable.
    app.include_router(targets.router, prefix="/targets")
    app.include_router(languages.router, prefix="/languages")
    app.include_router(
        importable_datasets.router, prefix="/importable-datasets"
    )
    app.include_router(data.router, prefix="/data")
    app.include_router(metadata.router, prefix="/metadata")


def _include_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def add_language_header(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        response = await call_next(request)
        if request.url.path.startswith("/metadata"):
            response.headers.setdefault("Content-Language", "no")
        return response


def _include_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(HTTPException)
    async def custom_http_exception_handler(
        _req: Request, exc: HTTPException
    ) -> JSONResponse:
        if exc.status_code == 404:
            logger.warning(exc, exc_info=True)
            return JSONResponse(
                content={"message": "Not found"}, status_code=404
            )
        logger.exception(exc)
        return JSONResponse(
            status_code=500, content={"message": "Internal Server Error"}
        )

    @app.exception_handler(NotFoundException)
    def handle_data_not_found(
        _req: Request, exc: NotFoundException
    ) -> JSONResponse:
        logger.warning(exc, exc_info=True)
        return JSONResponse(content={"message": "Not found"}, status_code=404)

    @app.exception_handler(InvalidDraftVersionException)
    def handle_invalid_draft(
        _req: Request, exc: InvalidDraftVersionException
    ) -> JSONResponse:
        logger.warning(exc, exc_info=True)
        return JSONResponse(content={"message": str(exc)}, status_code=404)

    @app.exception_handler(RequestValidationException)
    def handle_invalid_request(
        _req: Request, exc: RequestValidationException
    ) -> JSONResponse:
        logger.warning(exc, exc_info=True)
        return JSONResponse(content={"message": str(exc)}, status_code=400)

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        _req: Request, e: RequestValidationError
    ) -> JSONResponse:
        logger.warning("Validation error: %s", e, exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"message": "Bad Request", "details": e.errors()},
        )

    @app.exception_handler(InvalidStorageFormatException)
    def handle_invalid_format(
        _req: Request, exc: InvalidStorageFormatException
    ) -> JSONResponse:
        logger.exception(exc)
        return JSONResponse(
            content={"message": "Invalid storage format"}, status_code=500
        )

    @app.exception_handler(ValidationError)
    def handle_bad_request(_req: Request, e: ValidationError) -> JSONResponse:
        logger.warning(e, exc_info=True)
        return JSONResponse(status_code=400, content={"message": str(e)})

    @app.exception_handler(JobExistsException)
    def handle_job_exists(_req: Request, e: JobExistsException) -> JSONResponse:
        logger.warning(e, exc_info=True)
        return JSONResponse(status_code=400, content={"message": str(e)})

    @app.exception_handler(NameValidationError)
    def handle_invalid_name(
        _req: Request, e: NameValidationError
    ) -> JSONResponse:
        logger.warning(e, exc_info=True)
        return JSONResponse(status_code=400, content={"message": str(e)})

    @app.exception_handler(DatastoreNotFoundException)
    def handle_datastore_not_found(
        _req: Request, e: NameValidationError
    ) -> JSONResponse:
        logger.warning(e, exc_info=True)
        return JSONResponse(status_code=404, content={"message": str(e)})

    @app.exception_handler(AuthError)
    async def handle_auth_error(_req: Request, e: AuthError) -> JSONResponse:
        logger.warning(e, exc_info=True)
        return JSONResponse(
            status_code=401, content={"message": "Unauthorized"}
        )

    @app.exception_handler(Exception)
    def handle_generic_exception(_req: Request, exc: Exception) -> JSONResponse:
        logger.exception(exc)
        return JSONResponse(
            status_code=500, content={"message": "Internal Server Error"}
        )
