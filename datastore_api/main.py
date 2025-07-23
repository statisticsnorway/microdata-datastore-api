import logging

from fastapi.responses import JSONResponse
from fastapi import FastAPI, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from starlette.exceptions import HTTPException
from datastore_api.api import include_routers
from datastore_api.config.logging import setup_logging
from datastore_api.common.exceptions import (
    JobExistsException,
    NameValidationError,
    NotFoundException,
    InvalidStorageFormatException,
    RequestValidationException,
    InvalidDraftVersionException,
)


logger = logging.getLogger()

app = FastAPI()
setup_logging(app)
include_routers(app)


@app.middleware("http")
async def add_language_header(request, call_next):
    response = await call_next(request)
    response.headers.setdefault("Content-Language", "no")
    return response


@app.exception_handler(HTTPException)
async def custom_http_exception_handler(_req, exc):
    if exc.status_code == 404:
        return JSONResponse(
            content={
                "code": 103,
                "message": f"Error: {str(exc)}",
                "service": "datastore-api",
                "type": "PATH_NOT_FOUND",
            },
            status_code=400,
        )
    return JSONResponse(
        content={
            "code": 202,
            "message": f"Error: {str(exc)}",
            "service": "datastore-api",
            "type": "SYSTEM_ERROR",
        },
        status_code=500,
    )


@app.exception_handler(NotFoundException)
def handle_data_not_found(_req, exc):
    logger.warning(exc, exc_info=True)
    return JSONResponse(content={"message": "Not found"}, status_code=404)


@app.exception_handler(InvalidDraftVersionException)
def handle_invalid_draft(_req, exc):
    logger.warning(exc, exc_info=True)
    return JSONResponse(content={"message": str(exc)}, status_code=404)


@app.exception_handler(RequestValidationException)
def handle_invalid_request(_req, exc):
    logger.warning(exc, exc_info=True)
    return JSONResponse(content={"message": str(exc)}, status_code=400)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_req, e: RequestValidationError):
    logger.warning("Validation error: %s", e, exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"message": "Bad Request", "details": e.errors()},
    )


@app.exception_handler(InvalidStorageFormatException)
def handle_invalid_format(_req, exc):
    logger.exception(exc)
    return JSONResponse(
        content={"message": "Invalid storage format"}, status_code=500
    )


@app.exception_handler(ValidationError)
def handle_bad_request(_req, e: ValidationError):
    logger.warning(e, exc_info=True)
    return JSONResponse(status_code=400, content={"message": str(e)})


@app.exception_handler(JobExistsException)
def handle_job_exists(_req, e: JobExistsException):
    logger.warning(e, exc_info=True)
    return JSONResponse(status_code=400, content={"message": str(e)})


@app.exception_handler(NameValidationError)
def handle_invalid_name(_req, e: NameValidationError):
    logger.warning(e, exc_info=True)
    return JSONResponse(status_code=400, content={"message": str(e)})


@app.exception_handler(Exception)
def handle_generic_exception(_req, exc):
    logger.exception(exc)
    return JSONResponse(
        status_code=400, content={"message": "Internal Server Error"}
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "datastore_api.main:app", host="0.0.0.0", port=8000, reload=True
    )
