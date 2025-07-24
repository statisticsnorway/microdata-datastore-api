from fastapi import FastAPI

from datastore_api.config.logging import application, uvicorn


def setup_logging(app: FastAPI) -> None:
    application.setup_logging(app)
    uvicorn.setup_uvicorn_logging()
