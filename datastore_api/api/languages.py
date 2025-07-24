import logging

from fastapi import APIRouter

from datastore_api.domain import languages
from datastore_api.domain.languages import Language

logger = logging.getLogger()
router = APIRouter()


@router.get("/")
def get_languages() -> list[Language]:
    return languages.find_languages()
