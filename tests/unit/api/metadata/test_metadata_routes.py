import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from httpx import Response

from datastore_api.adapter import db
from datastore_api.common.models import Version
from datastore_api.domain import metadata
from datastore_api.main import app

MOCKED_DATASTORE_VERSIONS = {
    "name": "SSB-RAIRD",
    "label": "SSB datastore",
    "description": "Inneholder SSB databaser med data siden 1970 :-)",
    "versions": [
        {
            "version": "3.2.0.0",
            "description": "Denne versjonen handler om nye inntektsvariabler",
            "releaseTime": 123456,
            "languageCode": "no",
            "dataStructureUpdates": [
                {
                    "description": "string",
                    "name": "INNTEKT_TJENPEN",
                    "operation": "ADD",
                },
                {
                    "description": "string",
                    "name": "INNTEKT_BANKINNSK",
                    "operation": "ADD",
                },
            ],
            "updateType": "MINOR",
        }
    ],
    "languageCode": "no",
}

MOCKED_DATASTRUCTURE = {
    "description": "dummy",
    "name": "INNTEKT_TJENPEN",
    "operation": "ADD",
    "releaseTime": 123123,
}

MOCKED_DATASTRUCTURES = {
    "INNTEKT_TJENPEN": {
        "description": "dummy",
        "operation": "ADD",
        "releaseTime": 123123,
    },
    "INNTEKT_BANKINNSK": {
        "description": "dummy",
        "operation": "ADD",
        "releaseTime": 123123,
    },
}


MOCKED_LANGUAGES = [
    {"code": "no", "label": "Norsk"},
    {"code": "en", "label": "English"},
]

DATA_STRUCTURES_FILE_PATH = "tests/resources/fixtures/api/data_structures.json"
METADATA_ALL_FILE_PATH = "tests/resources/fixtures/domain/metadata_all.json"
DATASTORE_ROOT_DIR = Path("tests/resources/test_datastore")


@pytest.fixture
def mock_db_client():
    mock = Mock()
    mock.get_datastore.return_value = SimpleNamespace(
        directory=str("tests/resources/test_datastore")
    )
    return mock


@pytest.fixture
def client(mock_db_client: Mock):
    app.dependency_overrides[db.get_database_client] = lambda: mock_db_client
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_get_data_store(client, mocker):
    spy = mocker.patch.object(
        metadata,
        "find_all_datastore_versions",
        return_value=MOCKED_DATASTORE_VERSIONS,
    )
    response: Response = client.get(
        "/metadata/data-store",
        headers={
            "X-Request-ID": "test-123",
            "Accept-Language": "no",
            "Accept": "application/json",
        },
    )
    spy.assert_called_with(DATASTORE_ROOT_DIR)
    assert response.headers["Content-Type"] == "application/json"
    assert response.json() == MOCKED_DATASTORE_VERSIONS


def test_get_current_data_structure_status(client, mocker):
    spy = mocker.patch.object(
        metadata,
        "find_current_data_structure_status",
        return_value=MOCKED_DATASTRUCTURE,
    )
    response: Response = client.get(
        "metadata/data-structures/status?names=INNTEKT_TJENPEN",
        headers={
            "X-Request-ID": "test-123",
            "Accept-Language": "no",
            "Accept": "application/json",
        },
    )
    spy.assert_called_with([MOCKED_DATASTRUCTURE["name"]], DATASTORE_ROOT_DIR)
    assert response.headers["Content-Type"] == "application/json"
    assert response.json() == MOCKED_DATASTRUCTURE


def test_get_current_data_structure_status_as_post(client, mocker):
    spy = mocker.patch.object(
        metadata,
        "find_current_data_structure_status",
        return_value=MOCKED_DATASTRUCTURES,
    )
    response: Response = client.post(
        "metadata/data-structures/status",
        json={"names": ",".join(list(MOCKED_DATASTRUCTURES.keys()))},
        headers={
            "X-Request-ID": "test-123",
            "Accept-Language": "no",
            "Accept": "application/json",
        },
    )
    spy.assert_called_with(
        list(MOCKED_DATASTRUCTURES.keys()), DATASTORE_ROOT_DIR
    )
    assert response.headers["Content-Type"] == "application/json"
    assert response.json() == MOCKED_DATASTRUCTURES


def test_get_multiple_data_structure_status(client, mocker):
    spy = mocker.patch.object(
        metadata,
        "find_current_data_structure_status",
        return_value=MOCKED_DATASTRUCTURE,
    )
    response: Response = client.get(
        "/metadata/data-structures/status?names=INNTEKT_TJENPEN,INNTEKT_TO",
        headers={
            "X-Request-ID": "test-123",
            "Accept-Language": "no",
            "Accept": "application/json",
        },
    )
    spy.assert_called_with(
        ["INNTEKT_TJENPEN", "INNTEKT_TO"], DATASTORE_ROOT_DIR
    )
    assert response.headers["Content-Type"] == "application/json"
    assert response.json() == MOCKED_DATASTRUCTURE


def test_get_data_structures(client, mocker):
    with open(DATA_STRUCTURES_FILE_PATH, encoding="utf-8") as f:
        mocked_data_structures = json.load(f)
    spy = mocker.patch.object(
        metadata, "find_data_structures", return_value=mocked_data_structures
    )
    response: Response = client.get(
        "/metadata/data-structures?names=FNR,AKT_ARBAP&version=3.2.1.0",
        headers={
            "X-Request-ID": "test-123",
            "Accept-Language": "no",
            "Accept": "application/json",
        },
    )
    spy.assert_called_with(
        DATASTORE_ROOT_DIR,
        ["FNR", "AKT_ARBAP"],
        Version.from_str("3.2.1.0"),
        True,
        False,
    )
    assert response.headers["Content-Type"] == "application/json"
    assert response.json() == mocked_data_structures


def test_get_all_data_structures_ever(client, mocker):
    mocked_data_structures = ["TEST_PERSON_INCOME", "TEST_PERSON_PETS"]
    spy = mocker.patch.object(
        metadata,
        "find_all_data_structures_ever",
        return_value=mocked_data_structures,
    )
    response: Response = client.get(
        "/metadata/all-data-structures",
        headers={
            "X-Request-ID": "test-123",
            "Accept-Language": "no",
            "Accept": "application/json",
        },
    )
    spy.assert_called_with(DATASTORE_ROOT_DIR)
    assert response.headers["Content-Type"] == "application/json"
    assert response.json() == mocked_data_structures


def test_get_all_metadata(client, mocker):
    with open(DATA_STRUCTURES_FILE_PATH, encoding="utf-8") as f:
        mocked_data_structures = json.load(f)
    mocked_metadata_all = {
        "dataStore": {
            "name": "dummy datastore name",
            "label": "dummy datastore label",
            "description": "dummy datastore description",
            "languageCode": "no",
        },
        "languages": MOCKED_LANGUAGES,
        "dataStructures": mocked_data_structures,
    }
    spy = mocker.patch.object(
        metadata, "find_all_metadata", return_value=mocked_metadata_all
    )
    response: Response = client.get(
        "/metadata/all?version=3.2.1.0",
        headers={
            "X-Request-ID": "test-123",
            "Accept-Language": "no",
            "Accept": "application/json",
        },
    )
    spy.assert_called_with(
        Version.from_str("3.2.1.0"), DATASTORE_ROOT_DIR, False
    )
    assert response.headers["Content-Type"] == "application/json"
    assert response.json() == mocked_metadata_all


def test_get_all_metadata_long_version_numbers(client, mocker):
    with open(DATA_STRUCTURES_FILE_PATH, encoding="utf-8") as f:
        mocked_data_structures = json.load(f)
    mocked_metadata_all = {
        "dataStore": {
            "name": "dummy datastore name",
            "label": "dummy datastore label",
            "description": "dummy datastore description",
            "languageCode": "no",
        },
        "languages": MOCKED_LANGUAGES,
        "dataStructures": mocked_data_structures,
    }
    spy = mocker.patch.object(
        metadata, "find_all_metadata", return_value=mocked_metadata_all
    )
    response: Response = client.get(
        "/metadata/all?version=1234.5678.9012.0",
        headers={
            "X-Request-ID": "test-123",
            "Accept-Language": "no",
            "Accept": "application/json",
        },
    )
    spy.assert_called_with(
        Version.from_str("1234.5678.9012.0"), DATASTORE_ROOT_DIR, False
    )
    assert response.headers["Content-Type"] == "application/json"
    assert response.json() == mocked_metadata_all


def test_get_all_metadata_skip_code_lists(client, mocker):
    with open(METADATA_ALL_FILE_PATH, encoding="utf-8") as f:
        mocked_metadata_all = json.load(f)
    spy = mocker.patch.object(
        metadata, "find_all_metadata", return_value=mocked_metadata_all
    )
    response: Response = client.get(
        "/metadata/all?version=3.2.1.0&skip_code_lists=true",
        headers={
            "X-Request-ID": "test-123",
            "Accept-Language": "no",
            "Accept": "application/json",
        },
    )
    spy.assert_called_with(
        Version.from_str("3.2.1.0"), DATASTORE_ROOT_DIR, True
    )
    assert response.headers["Content-Type"] == "application/json"
    assert response.json() == mocked_metadata_all


def test_get_data_structures_skip_code_lists(client, mocker):
    with open(DATA_STRUCTURES_FILE_PATH, encoding="utf-8") as f:
        mocked_data_structures = json.load(f)
    spy = mocker.patch.object(
        metadata, "find_data_structures", return_value=mocked_data_structures
    )
    response: Response = client.get(
        "/metadata/data-structures?names=FNR,AKT_ARBAP&version=3.2.1.0&skip_code_lists=true",
        headers={
            "X-Request-ID": "test-123",
            "Accept-Language": "no",
            "Accept": "application/json",
        },
    )
    spy.assert_called_with(
        DATASTORE_ROOT_DIR,
        ["FNR", "AKT_ARBAP"],
        Version.from_str("3.2.1.0"),
        True,
        True,
    )
    assert response.headers["Content-Type"] == "application/json"
    assert response.json() == mocked_data_structures
