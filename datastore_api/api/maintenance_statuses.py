import logging

from fastapi import APIRouter, Depends

from datastore_api.adapter import db
from datastore_api.common.models import CamelModel

logger = logging.getLogger()
router = APIRouter()


class NewMaintenanceStatusRequest(CamelModel, extra="forbid"):
    msg: str
    paused: bool


@router.post("/")
def set_status(
    maintenance_status_request: NewMaintenanceStatusRequest,
    database_client: db.DatabaseClient = Depends(db.get_database_client),
):
    return database_client.set_maintenance_status(
        maintenance_status_request.msg, maintenance_status_request.paused
    )


@router.get("/")
def get_history(
    database_client: db.DatabaseClient = Depends(db.get_database_client),
):
    return database_client.get_maintenance_history()


@router.get("/latest")
def get_status(
    database_client: db.DatabaseClient = Depends(db.get_database_client),
):
    return database_client.get_latest_maintenance_status()
