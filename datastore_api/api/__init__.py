import logging

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.exceptions import HTTPException

from datastore_api.api import (
    importable_datasets,
    jobs,
    maintenance_status,
    targets,
)
from datastore_api.api.data import data_router
from datastore_api.api.metadata import metadata_router
from datastore_api.api.observability import observability_router
from datastore_api.common.exceptions import (
    InvalidDraftVersionException,
    InvalidStorageFormatException,
    JobExistsException,
    NameValidationError,
    NotFoundException,
    RequestValidationException,
)

logger = logging.getLogger()


def setup_api(app: FastAPI):
    _include_exception_handlers(app)
    _include_middleware(app)
    _include_routers(app)


def _include_routers(app: FastAPI):
    app.include_router(data_router)
    app.include_router(metadata_router)
    app.include_router(observability_router)
    app.include_router(maintenance_status.router)
    app.include_router(targets.router)
    app.include_router(importable_datasets.router)
    app.include_router(jobs.router)


def _include_middleware(app: FastAPI):
    @app.middleware("http")
    async def add_language_header(request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/metadata"):
            response.headers.setdefault("Content-Language", "no")
        return response


def _include_exception_handlers(app: FastAPI):
    @app.exception_handler(HTTPException)
    async def custom_http_exception_handler(_req: Request, exc):
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
    def handle_data_not_found(_req: Request, exc):
        logger.warning(exc, exc_info=True)
        return JSONResponse(content={"message": "Not found"}, status_code=404)

    @app.exception_handler(InvalidDraftVersionException)
    def handle_invalid_draft(_req: Request, exc):
        logger.warning(exc, exc_info=True)
        return JSONResponse(content={"message": str(exc)}, status_code=404)

    @app.exception_handler(RequestValidationException)
    def handle_invalid_request(_req: Request, exc):
        logger.warning(exc, exc_info=True)
        return JSONResponse(content={"message": str(exc)}, status_code=400)

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        _req: Request, e: RequestValidationError
    ):
        logger.warning("Validation error: %s", e, exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"message": "Bad Request", "details": e.errors()},
        )

    @app.exception_handler(InvalidStorageFormatException)
    def handle_invalid_format(_req: Request, exc):
        logger.exception(exc)
        return JSONResponse(
            content={"message": "Invalid storage format"}, status_code=500
        )

    @app.exception_handler(ValidationError)
    def handle_bad_request(_req: Request, e: ValidationError):
        logger.warning(e, exc_info=True)
        return JSONResponse(status_code=400, content={"message": str(e)})

    @app.exception_handler(JobExistsException)
    def handle_job_exists(_req: Request, e: JobExistsException):
        logger.warning(e, exc_info=True)
        return JSONResponse(status_code=400, content={"message": str(e)})

    @app.exception_handler(NameValidationError)
    def handle_invalid_name(_req: Request, e: NameValidationError):
        logger.warning(e, exc_info=True)
        return JSONResponse(status_code=400, content={"message": str(e)})

    @app.exception_handler(Exception)
    def handle_generic_exception(_req: Request, exc):
        logger.exception(exc)
        return JSONResponse(
            status_code=500, content={"message": "Internal Server Error"}
        )
