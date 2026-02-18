import os
import shutil
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from datastore_api.adapter import db
from datastore_api.adapter.auth.dependencies import authorize_data_administrator
from datastore_api.adapter.db.models import Datastore
from datastore_api.main import app

DATASTORE_RDN = "no.dev.test"
DATASTORE = Datastore(
    datastore_id=1,
    rdn=DATASTORE_RDN,
    description="Datastore for testing",
    directory="tests/resources/test_datastore",
    name="Test datastore",
    bump_enabled=False,
)


@pytest.fixture
def mock_db_client():
    mock = Mock()
    mock.get_datastore_id_from_rdn.return_value = 1
    mock.get_datastore.return_value = DATASTORE

    mock.get_jobs.return_value = []
    return mock


@pytest.fixture
def mock_auth_deps():
    return {
        "data_administrator": Mock(return_value=None),
    }


@pytest.fixture
def client(mock_db_client, mock_auth_deps):
    app.dependency_overrides[db.get_database_client] = lambda: mock_db_client
    app.dependency_overrides[authorize_data_administrator] = lambda: (
        mock_auth_deps["data_administrator"]()
    )
    yield TestClient(app)
    app.dependency_overrides.clear()


def teardown_module():
    os.remove(
        "tests/resources/test_datastore_input/DATASET_WITH_INVAL&D_NAM+E.tar"
    )


def test_get_files(client, mock_db_client, mock_auth_deps):
    response = client.get(f"/datastores/{DATASTORE_RDN}/importable-datasets")
    mock_auth_deps["data_administrator"].assert_called_once()
    mock_db_client.get_datastore.assert_called_once()
    assert response.status_code == 200
    assert len(response.json()) == 4
    expected_datasets = [
        {
            "datasetName": "MY_DATASET",
            "hasData": True,
            "hasMetadata": True,
            "isArchived": False,
        },
        {
            "datasetName": "YOUR_DATASET",
            "hasData": False,
            "hasMetadata": True,
            "isArchived": False,
        },
        {
            "datasetName": "OTHER_DATASET",
            "hasData": True,
            "hasMetadata": True,
            "isArchived": False,
        },
        {
            "datasetName": "YET_ANOTHER_DATASET",
            "hasData": True,
            "hasMetadata": True,
            "isArchived": True,
        },
    ]
    for dataset in expected_datasets:
        assert dataset in response.json()


def test_get_invalid_name_files(client, mock_db_client):
    shutil.copyfile(
        "tests/resources/test_datastore_input/MY_DATASET.tar",
        "tests/resources/test_datastore_input/DATASET_WITH_INVAL&D_NAM+E.tar",
    )
    response = client.get(f"/datastores/{DATASTORE_RDN}/importable-datasets")
    mock_db_client.get_datastore_id_from_rdn.assert_called_once_with(
        DATASTORE_RDN
    )
    mock_db_client.get_datastore.assert_called_once_with(1)
    mock_db_client.get_jobs.assert_called_once()

    assert response.status_code == 200
    assert len(response.json()) == 4
    expected_datasets = [
        {
            "datasetName": "MY_DATASET",
            "hasData": True,
            "hasMetadata": True,
            "isArchived": False,
        },
        {
            "datasetName": "YOUR_DATASET",
            "hasData": False,
            "hasMetadata": True,
            "isArchived": False,
        },
        {
            "datasetName": "OTHER_DATASET",
            "hasData": True,
            "hasMetadata": True,
            "isArchived": False,
        },
        {
            "datasetName": "YET_ANOTHER_DATASET",
            "hasData": True,
            "hasMetadata": True,
            "isArchived": True,
        },
    ]
    for dataset in expected_datasets:
        assert dataset in response.json()


def test_delete_importable_datasets_api(client, mock_db_client, mock_auth_deps):
    open(
        "tests/resources/test_datastore_input/DATASET_THAT_SHOULD_BE_DELETED.tar",
        "x",
    )
    response = client.delete(
        f"/datastores/{DATASTORE_RDN}/importable-datasets/DATASET_THAT_SHOULD_BE_DELETED",
    )
    mock_auth_deps["data_administrator"].assert_called_once()
    mock_db_client.get_datastore.assert_called_once()
    assert response.status_code == 200
    assert not os.path.exists(
        "tests/resources/test_datastore_input/DATASET_THAT_SHOULD_BE_DELETED.tar"
    )


def test_delete_nonexisting_dataset(client, mock_db_client):
    response = client.delete(
        f"/datastores/{DATASTORE_RDN}/importable-datasets/NONEXISTING_DATASET",
    )
    assert response.status_code == 404
    mock_db_client.get_datastore.assert_called_once()


def test_delete_invalid_name_dataset(client, mock_db_client):
    response = client.delete(
        f"/datastores/{DATASTORE_RDN}/importable-datasets/INVALID_NAME_DATASET++"
    )
    assert response.status_code == 400
    mock_db_client.get_datastore.assert_called_once()
