import json
import logging
import os
from typing import Union

from pyarrow import parquet

from datastore_api.config import environment
from datastore_api.common.exceptions import DataNotFoundException


DATASTORE_ROOT_DIR = environment.get("DATASTORE_ROOT_DIR")
DATA_DIR = f"{DATASTORE_ROOT_DIR}/data"

logger = logging.getLogger()


def get_parquet_file_path(dataset_name: str, version: str) -> str:
    path_prefix = f"{DATA_DIR}/{dataset_name}"
    if version == "0_0":
        full_path = _get_draft_file_path(path_prefix, dataset_name)
        if full_path is None:
            logger.info(f"No DRAFT for {dataset_name}. Using latest version")
            version = _get_latest_version()
        else:
            _log_parquet_info(full_path)
            return full_path

    file_name = _get_file_name_from_data_versions(version, dataset_name)
    full_path = f"{path_prefix}/{file_name}"

    if not os.path.exists(full_path):
        logger.error(f"{full_path} does not exist")
        raise DataNotFoundException(
            f"No file exists for {dataset_name} in version {version}"
        )
    _log_parquet_info(full_path)
    return full_path


def _get_file_name_from_data_versions(version: str, dataset_name: str) -> str:
    data_versions_file = (
        f"{DATASTORE_ROOT_DIR}/datastore/data_versions__{version}.json"
    )
    with open(data_versions_file, encoding="utf-8") as f:
        data_versions = json.load(f)

    if dataset_name not in data_versions:
        raise DataNotFoundException(
            f"No {dataset_name} in data_versions file for version {version}"
        )
    return data_versions[dataset_name]


def _get_draft_file_path(
    path_prefix: str, dataset_name: str
) -> Union[None, str]:
    partitioned_parquet_path = f"{path_prefix}/{dataset_name}__DRAFT"
    parquet_path = f"{partitioned_parquet_path}.parquet"
    if os.path.isfile(parquet_path):
        return parquet_path
    elif os.path.isdir(partitioned_parquet_path):
        return partitioned_parquet_path
    else:
        return None


def _get_latest_version():
    datastore_files = os.listdir(f"{DATASTORE_ROOT_DIR}/datastore")
    sem_ver = [
        (int(str_version.split("_")[0]), int(str_version.split("_")[1]))
        for str_version in [
            file[15:-5]
            for file in datastore_files
            if file.startswith("data_versions")
        ]
    ]
    sem_ver.sort()
    major, minor = sem_ver[-1][0], sem_ver[-1][1]
    return f"{major}_{minor}"


def _log_parquet_info(parquet_file):
    if os.path.isdir(parquet_file):
        _log_info_partitioned_parquet(parquet_file)
    else:
        _log_parquet_details(parquet_file)


def _log_info_partitioned_parquet(parquet_file):
    for subdir, _, files in os.walk(parquet_file):
        for filename in files:
            filepath = subdir + os.sep + filename
            if filepath.endswith(".parquet"):
                _log_parquet_details(filepath)
                # Log just the first file
                return


def _log_parquet_details(parquet_file):
    logger.info(
        f"Parquet file: {parquet_file} "
        f"Parquet metadata: {parquet.read_metadata(parquet_file)} "
        f"Parquet schema: {parquet.read_schema(parquet_file).to_string()}"
    )
