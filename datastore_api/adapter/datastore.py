import json
import logging
from functools import lru_cache

from datastore_api.config import environment
from datastore_api.domain.version import Version
from datastore_api.exceptions.exceptions import DataNotFoundException

DATASTORE_ROOT_DIR = environment.get("DATASTORE_ROOT_DIR")

logger = logging.getLogger()


def get_draft_version() -> dict:
    json_file = f"{DATASTORE_ROOT_DIR}/datastore/draft_version.json"
    with open(json_file, encoding="utf-8") as f:
        return json.load(f)


def get_datastore_versions() -> dict:
    datastore_versions_json = (
        f"{DATASTORE_ROOT_DIR}/datastore/datastore_versions.json"
    )
    with open(datastore_versions_json, encoding="utf-8") as f:
        return json.load(f)


def _get_draft_metadata_all() -> dict:
    metadata_all_file_path = (
        f"{DATASTORE_ROOT_DIR}/datastore/metadata_all__DRAFT.json"
    )
    with open(metadata_all_file_path, "r", encoding="utf-8") as f:
        return json.load(f)


@lru_cache(maxsize=32)
def _get_versioned_metadata_all(version: Version) -> dict:
    file_version = version.to_3_underscored()
    metadata_all_file_path = (
        f"{DATASTORE_ROOT_DIR}/datastore/metadata_all__{file_version}.json"
    )
    with open(metadata_all_file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_metadata_all(version: Version) -> dict:
    try:
        if version.is_draft():
            return _get_draft_metadata_all()
        else:
            result = _get_versioned_metadata_all(version)
            cache_info = _get_versioned_metadata_all.cache_info()
            logger.info(
                f"Cache info for versioned metadata: hits={cache_info.hits}, misses={cache_info.misses}, currsize={cache_info.currsize}"
            )
            return result
    except FileNotFoundError as e:
        raise DataNotFoundException(
            f"metadata_all for version {version} not found"
        ) from e
