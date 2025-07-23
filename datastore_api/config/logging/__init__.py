from fastapi import FastAPI
from datastore_api.config.logging import uvicorn, application


def setup_logging(app: FastAPI):
    application.setup_logging(app)
    uvicorn.setup_uvicorn_logging()
