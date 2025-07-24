import json
from itertools import chain
from typing import List

import pytest

from datastore_api.adapter.local_storage import datastore_directory
from datastore_api.common.exceptions import (
    InvalidDraftVersionException,
    InvalidStorageFormatException,
)
from datastore_api.common.models import Version
from datastore_api.config import environment
from datastore_api.domain import metadata

DATASTORE_ROOT_DIR = environment.datastore_root_dir
FIXTURES_DIR = "tests/resources/fixtures"
METADATA_ALL_FILE_PATH = f"{FIXTURES_DIR}/domain/metadata_all.json"
DATASTORE_VERSIONS_FILE_PATH = f"{FIXTURES_DIR}/domain/datastore_versions.json"
DRAFT_VERSION_FILE_PATH = f"{DATASTORE_ROOT_DIR}/datastore/draft_version.json"
DATA_STRUCTURES_FILE_PATH = f"{FIXTURES_DIR}/api/data_structures.json"

METADATA_ALL_NO_CODE_LIST_FILE_PATH = (
    f"{FIXTURES_DIR}/domain/metadata_all_no_code_list__1_0_0_0.json"
)

DATA_STRUCTURES_NO_CODE_LIST_FILE_PATH = (
    f"{FIXTURES_DIR}/api/data_structures_no_code_list.json"
)


def test_find_two_data_structures_with_attrs(mocker):
    with open(METADATA_ALL_FILE_PATH, encoding="utf-8") as f:
        mocked_metadata_all = json.load(f)
    mocker.patch.object(
        datastore_directory,
        "get_metadata_all",
        return_value=mocked_metadata_all,
    )
    actual = metadata.find_data_structures(
        ["TEST_PERSON_INCOME", "TEST_PERSON_PETS"],
        Version.from_str("1.0.0.0"),
        True,
        skip_code_lists=False,
    )
    assert len(actual) == 2
    income = next(
        data_structure
        for data_structure in mocked_metadata_all["dataStructures"]
        if data_structure["name"] == "TEST_PERSON_INCOME"
    )
    assert "attributeVariables" in income
    pets = next(
        data_structure
        for data_structure in mocked_metadata_all["dataStructures"]
        if data_structure["name"] == "TEST_PERSON_PETS"
    )
    assert "attributeVariables" in pets


def test_find_two_data_structures_without_attrs(mocker):
    with open(METADATA_ALL_FILE_PATH, encoding="utf-8") as f:
        mocked_metadata_all = json.load(f)
    mocker.patch.object(
        datastore_directory,
        "get_metadata_all",
        return_value=mocked_metadata_all,
    )
    actual = metadata.find_data_structures(
        ["TEST_PERSON_INCOME", "TEST_PERSON_PETS"],
        Version.from_str("1.0.0.0"),
        False,
        skip_code_lists=False,
    )
    assert len(actual) == 2
    income = next(
        data_structure
        for data_structure in mocked_metadata_all["dataStructures"]
        if data_structure["name"] == "TEST_PERSON_INCOME"
    )
    assert "attributeVariables" not in income
    pets = next(
        data_structure
        for data_structure in mocked_metadata_all["dataStructures"]
        if data_structure["name"] == "TEST_PERSON_PETS"
    )
    assert "attributeVariables" not in pets


def test_find_data_structures_no_name_filter(mocker):
    with open(METADATA_ALL_FILE_PATH, encoding="utf-8") as f:
        mocked_metadata_all = json.load(f)
    mocker.patch.object(
        datastore_directory,
        "get_metadata_all",
        return_value=mocked_metadata_all,
    )
    actual = metadata.find_data_structures(
        [], Version.from_str("1.0.0.0"), True, skip_code_lists=False
    )
    assert len(actual) == 2


def test_find_current_data_structure_status(mocker):
    with open(DATASTORE_VERSIONS_FILE_PATH, encoding="utf-8") as f:
        mocked_datastore_versions = json.load(f)
    with open(DRAFT_VERSION_FILE_PATH, encoding="utf-8") as f:
        mocked_draft_version = json.load(f)
    mocker.patch.object(
        datastore_directory,
        "get_datastore_versions",
        return_value=mocked_datastore_versions,
    )
    mocker.patch.object(
        datastore_directory,
        "get_draft_version",
        return_value=mocked_draft_version,
    )
    actual_draft = metadata.find_current_data_structure_status([
        "TEST_PERSON_HOBBIES"
    ])
    actual_pending_release = metadata.find_current_data_structure_status([
        "TEST_PERSON_SAVINGS"
    ])
    actual_released = metadata.find_current_data_structure_status([
        "TEST_PERSON_PETS"
    ])
    actual_removed = metadata.find_current_data_structure_status([
        "TEST_PERSON_INCOME"
    ])
    actual_no_such_dataset = metadata.find_current_data_structure_status([
        "NO_SUCH_DATASET"
    ])
    actual_all = metadata.find_current_data_structure_status([
        "TEST_PERSON_INCOME",
        "TEST_PERSON_PETS",
        "TEST_PERSON_SAVINGS",
        "TEST_PERSON_HOBBIES",
        "NO_SUCH_DATASET",
    ])
    expected_draft = {
        "TEST_PERSON_HOBBIES": {
            "operation": "ADD",
            "releaseTime": 1608000000,
            "releaseStatus": "DRAFT",
        }
    }
    expected_no_such_dataset = {"NO_SUCH_DATASET": None}
    expected_pending_release = {
        "TEST_PERSON_SAVINGS": {
            "operation": "ADD",
            "releaseTime": 1608000000,
            "releaseStatus": "PENDING_RELEASE",
        }
    }
    expected_released = {
        "TEST_PERSON_PETS": {
            "operation": "ADD",
            "releaseTime": 1607332752,
            "releaseStatus": "RELEASED",
        }
    }
    expected_removed = {
        "TEST_PERSON_INCOME": {
            "operation": "REMOVE",
            "releaseTime": 1607332762,
            "releaseStatus": "DELETED",
        }
    }
    expected_all = {
        **expected_draft,
        **expected_pending_release,
        **expected_released,
        **expected_removed,
        **expected_no_such_dataset,
    }
    assert actual_draft == expected_draft
    assert actual_pending_release == expected_pending_release
    assert actual_released == expected_released
    assert actual_removed == expected_removed
    assert actual_no_such_dataset == expected_no_such_dataset
    assert actual_all == expected_all


def test_find_all_datastore_versions(mocker):
    with open(DATASTORE_VERSIONS_FILE_PATH, encoding="utf-8") as f:
        mocked_datastore_versions = json.load(f)
    with open(DRAFT_VERSION_FILE_PATH, encoding="utf-8") as f:
        mocked_draft_version = json.load(f)
    mocker.patch.object(
        datastore_directory,
        "get_datastore_versions",
        return_value=mocked_datastore_versions,
    )
    mocker.patch.object(
        datastore_directory,
        "get_draft_version",
        return_value=mocked_draft_version,
    )
    actual = metadata.find_all_datastore_versions()
    assert len(actual["versions"]) == 3
    assert actual["versions"][0]["version"] == "0.0.0.1608000000"
    assert actual["versions"][1]["version"] == "2.0.0.0"


def test_find_all_datastore_versions_when_draft_version_empty(mocker):
    with open(DATASTORE_VERSIONS_FILE_PATH, encoding="utf-8") as f:
        mocked_datastore_versions = json.load(f)
    mocker.patch.object(
        datastore_directory,
        "get_datastore_versions",
        return_value=mocked_datastore_versions,
    )
    mocker.patch.object(
        datastore_directory, "get_draft_version", return_value={}
    )
    actual = metadata.find_all_datastore_versions()
    assert len(actual["versions"]) == 2
    assert actual["versions"][0]["version"] == "2.0.0.0"


def test_find_all_data_structures_ever(mocker):
    with open(DATASTORE_VERSIONS_FILE_PATH, encoding="utf-8") as f:
        mocked_datastore_versions = json.load(f)
    with open(DRAFT_VERSION_FILE_PATH, encoding="utf-8") as f:
        mocked_draft_version = json.load(f)
    mocker.patch.object(
        datastore_directory,
        "get_datastore_versions",
        return_value=mocked_datastore_versions,
    )
    mocker.patch.object(
        datastore_directory,
        "get_draft_version",
        return_value=mocked_draft_version,
    )

    actual = metadata.find_all_data_structures_ever()
    assert len(actual) == 4
    assert isinstance(actual, List)


def test_get_metadata_all_skip_code_list_and_missing_values(mocker):
    with open(METADATA_ALL_FILE_PATH, encoding="utf-8") as f:
        mocked_metadata_all = json.load(f)

    mocker.patch.object(
        datastore_directory,
        "get_metadata_all",
        return_value=mocked_metadata_all,
    )
    filtered_metadata = (
        metadata.find_all_metadata_skip_code_list_and_missing_values(
            Version.from_str("1.0.0.0")
        )
    )
    _assert_code_list_and_missing_values(filtered_metadata["dataStructures"])
    with open(METADATA_ALL_NO_CODE_LIST_FILE_PATH, encoding="utf-8") as f:
        metadata_no_code_list = json.load(f)
    assert metadata_no_code_list == filtered_metadata


def test_find_all_metadata_skip_code_list_and_missing_values_invalid_model(
    mocker,
):
    with open(DATA_STRUCTURES_FILE_PATH, encoding="utf-8") as f:
        mocked_data_structures = json.load(f)

    mocker.patch.object(
        datastore_directory,
        "get_metadata_all",
        return_value=mocked_data_structures,
    )
    with pytest.raises(InvalidStorageFormatException) as e:
        metadata.find_all_metadata_skip_code_list_and_missing_values(
            Version.from_str("1.0.0.0")
        )
    assert "Invalid metadata format" in str(e)


def test_get_draft_metadata_all(mocker):
    with open(METADATA_ALL_FILE_PATH, encoding="utf-8") as f:
        mocked_metadata_all = json.load(f)

    mocker.patch.object(
        datastore_directory,
        "get_metadata_all",
        return_value=mocked_metadata_all,
    )
    filtered_metadata = metadata.find_all_metadata(
        Version.from_str("0.0.0.1608000000")
    )

    assert "dataStructures" in filtered_metadata


def test_get_draft_metadata_all_0_0_0_0(mocker):
    with open(METADATA_ALL_FILE_PATH, encoding="utf-8") as f:
        mocked_metadata_all = json.load(f)

    mocker.patch.object(
        datastore_directory,
        "get_metadata_all",
        return_value=mocked_metadata_all,
    )
    filtered_metadata = metadata.find_all_metadata(Version.from_str("0.0.0.0"))

    assert "dataStructures" in filtered_metadata


def test_get_draft_metadata_all_invalid_draft_version(mocker):
    with open(METADATA_ALL_FILE_PATH, encoding="utf-8") as f:
        mocked_metadata_all = json.load(f)

    mocker.patch.object(
        datastore_directory,
        "get_metadata_all",
        return_value=mocked_metadata_all,
    )

    with pytest.raises(InvalidDraftVersionException) as e:
        metadata.find_all_metadata(Version.from_str("0.0.0.2"))

    assert "Requested draft version" in str(e)


def _assert_code_list_and_missing_values(metadata_all):
    represented_variables = []
    for metadata_dict in metadata_all:
        represented_measure = metadata_dict["measureVariable"][
            "representedVariables"
        ]
        represented_identifiers = list(
            chain(*[
                identifier["representedVariables"]
                for identifier in metadata_dict["identifierVariables"]
            ])
        )
        represented_attributes = list(
            chain(*[
                attribute["representedVariables"]
                for attribute in metadata_dict["attributeVariables"]
            ])
        )
        represented_variables += (
            represented_measure
            + represented_identifiers
            + represented_attributes
        )
    assert all([
        variable["valueDomain"].get("codeList", []) == []
        for variable in represented_variables
    ])
    assert all([
        variable["valueDomain"].get("missingValues", []) == []
        for variable in represented_variables
    ])
