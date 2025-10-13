import json
import logging
import os
from functools import lru_cache
from pathlib import Path

from datastore_api.common.exceptions import NotFoundException
from datastore_api.common.models import Version

logger = logging.getLogger()


def get_draft_version(datastore_root_dir: Path) -> dict:
    json_file = f"{datastore_root_dir}/datastore/draft_version.json"
    with open(json_file, encoding="utf-8") as f:
        return json.load(f)


def get_datastore_versions(datastore_root_dir: Path) -> dict:
    datastore_versions_json = (
        f"{datastore_root_dir}/datastore/datastore_versions.json"
    )
    with open(datastore_versions_json, encoding="utf-8") as f:
        return json.load(f)


def _get_draft_metadata_all(datastore_root_dir: Path) -> dict:
    metadata_all_file_path = (
        f"{datastore_root_dir}/datastore/metadata_all__DRAFT.json"
    )
    with open(metadata_all_file_path, "r", encoding="utf-8") as f:
        return json.load(f)


@lru_cache(maxsize=32)
def _get_versioned_metadata_all(
    version: Version, datastore_root_dir: Path
) -> dict:
    file_version = version.to_3_underscored()
    metadata_all_file_path = (
        f"{datastore_root_dir}/datastore/metadata_all__{file_version}.json"
    )
    with open(metadata_all_file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_metadata_all(version: Version, datastore_root_dir: Path) -> dict:
    try:
        if version.is_draft():
            return _get_draft_metadata_all(datastore_root_dir)
        else:
            result = _get_versioned_metadata_all(version, datastore_root_dir)
            cache_info = _get_versioned_metadata_all.cache_info()
            logger.info(
                f"Cache info for versioned metadata: hits={cache_info.hits}, "
                + "misses={cache_info.misses}, currsize={cache_info.currsize}"
            )
            return result
    except FileNotFoundError as e:
        raise NotFoundException(
            f"metadata_all for version {version} not found"
        ) from e


def get_draft_data_file_path(
    dataset_name: str, datastore_root_dir: Path
) -> str | None:
    dataset_dir = f"{datastore_root_dir}/data/{dataset_name}"
    partitioned_parquet_path = f"{dataset_dir}/{dataset_name}__DRAFT"
    parquet_path = f"{partitioned_parquet_path}.parquet"
    if os.path.isfile(parquet_path):
        return parquet_path
    elif os.path.isdir(partitioned_parquet_path):
        return partitioned_parquet_path
    else:
        return None


def get_data_path_from_data_versions(
    dataset_name: str, version: Version, datastore_root_dir: Path
) -> str:
    file_version = version.to_2_underscored()
    data_versions_file = (
        f"{datastore_root_dir}/datastore/data_versions__{file_version}.json"
    )
    with open(data_versions_file, encoding="utf-8") as f:
        data_versions = json.load(f)
    if dataset_name not in data_versions:
        raise NotFoundException(
            f"No {dataset_name} in data_versions file "
            + f"for version {file_version}"
        )
    file_name = data_versions[dataset_name]
    full_path = f"{datastore_root_dir}/data/{dataset_name}/{file_name}"
    if not os.path.exists(full_path):
        logger.error(f"{full_path} does not exist")
        raise NotFoundException(
            f"No file exists for {dataset_name} in version {version}"
        )
    return full_path


def get_latest_version(datastore_root_dir: Path) -> Version:
    datastore_versions = json.load(
        open(f"{datastore_root_dir}/datastore/datastore_versions.json")
    )
    version_list = datastore_versions.get("versions", [])
    return Version.from_str((version_list[0] or {}).get("version", ""))
