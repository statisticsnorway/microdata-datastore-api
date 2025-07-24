import logging

from fastapi import APIRouter, Depends

from datastore_api.adapter import db
from datastore_api.common.models import CamelModel

logger = logging.getLogger()

router = APIRouter()


class MaintenanceStatusRequest(CamelModel, extra="forbid"):
    msg: str
    paused: bool


@router.post("/maintenance-status")
def set_status(
    maintenance_status_request: MaintenanceStatusRequest,
    database_client: db.DatabaseClient = Depends(db.get_database_client),
):
    new_status = database_client.set_maintenance_status(
        maintenance_status_request.msg, maintenance_status_request.paused
    )
    return new_status


@router.get("/maintenance-status")
def get_status(
    database_client: db.DatabaseClient = Depends(db.get_database_client),
):
    document = database_client.get_latest_maintenance_status()
    if "paused" in document and document["paused"]:
        logger.info(
            f"GET /maintenance-status, paused: {document['paused']}, msg: {document['msg']}"
        )
    return document


@router.get("/maintenance-history")
def get_history(
    database_client: db.DatabaseClient = Depends(db.get_database_client),
):
    documents = database_client.get_maintenance_history()
    return documents
