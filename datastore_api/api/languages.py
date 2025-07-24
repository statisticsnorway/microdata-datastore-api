import logging

from fastapi import APIRouter

from datastore_api.domain import metadata

logger = logging.getLogger()
router = APIRouter()


@router.get("/")
def get_languages() -> list[dict[str, str]]:
    return metadata.find_languages()
