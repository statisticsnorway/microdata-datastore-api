from fastapi import APIRouter, Depends

from datastore_api.adapter import db
from datastore_api.adapter.db.models import Datastore
from datastore_api.api.common.dependencies import get_datastore_id

router = APIRouter()


@router.get("")
async def get_datastore(
    db_client: db.DatabaseClient = Depends(db.get_database_client),
    datastore_id: int = Depends(get_datastore_id),
) -> Datastore:
    return db_client.get_datastore(datastore_id)
