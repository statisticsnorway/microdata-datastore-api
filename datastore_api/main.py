import logging

from fastapi.responses import JSONResponse
from fastapi import FastAPI
from starlette.exceptions import HTTPException
from datastore_api.api.metadata_api import metadata_router
from datastore_api.api.observability import observability_router
from datastore_api.config.logging import setup_logging
from datastore_api.config.uvicorn import setup_uvicorn_logging
from datastore_api.exceptions.exceptions import (
    DataNotFoundException,
    InvalidStorageFormatException,
    RequestValidationException,
    InvalidDraftVersionException,
)


logger = logging.getLogger()

app = FastAPI()
app.include_router(observability_router)
app.include_router(metadata_router)

setup_logging(app)
setup_uvicorn_logging()


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
                "service": "metadata-service",
                "type": "PATH_NOT_FOUND",
            },
            status_code=400,
        )
    return JSONResponse(
        content={
            "code": 202,
            "message": f"Error: {str(exc)}",
            "service": "metadata-service",
            "type": "SYSTEM_ERROR",
        },
        status_code=500,
    )


@app.exception_handler(Exception)
def handle_generic_exception(_req, exc):
    logger.exception(exc)
    return JSONResponse(
        content={
            "code": 202,
            "message": f"Error: {str(exc)}",
            "service": "metadata-service",
            "type": "SYSTEM_ERROR",
        },
        status_code=500,
    )


@app.exception_handler(DataNotFoundException)
def handle_data_not_found(_req, exc):
    logger.warning(exc, exc_info=True)
    return JSONResponse(content=exc.to_dict(), status_code=404)


@app.exception_handler(InvalidDraftVersionException)
def handle_invalid_draft(_req, exc):
    logger.warning(exc, exc_info=True)
    return JSONResponse(content={"message": str(exc)}, status_code=404)


@app.exception_handler(RequestValidationException)
def handle_invalid_request(_req, exc):
    logger.warning(exc, exc_info=True)
    return JSONResponse(content=exc.to_dict(), status_code=400)


@app.exception_handler(InvalidStorageFormatException)
def handle_invalid_format(_req, exc):
    logger.exception(exc)
    return JSONResponse(content=exc.to_dict(), status_code=500)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "datastore_api.main:app", host="0.0.0.0", port=8000, reload=True
    )
