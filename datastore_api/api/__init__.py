from fastapi import FastAPI

from datastore_api.api.data import data_router
from datastore_api.api.metadata import metadata_router
from datastore_api.api.observability import observability_router
from datastore_api.api import maintenance_status
from datastore_api.api import targets
from datastore_api.api import importable_datasets
from datastore_api.api import jobs


def include_routers(app: FastAPI):
    app.include_router(data_router)
    app.include_router(metadata_router)
    app.include_router(observability_router)
    app.include_router(maintenance_status.router)
    app.include_router(targets.router)
    app.include_router(importable_datasets.router)
    app.include_router(jobs.router)
