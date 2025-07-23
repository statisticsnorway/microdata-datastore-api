import os
import shutil

from fastapi.testclient import TestClient

from datastore_api.main import app

client = TestClient(app)


def teardown_module():
    os.remove(
        "tests/resources/test_datastore_input/DATASET_WITH_INVAL&D_NAM+E.tar"
    )


def test_get_files():
    response = client.get("/importable-datasets")
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


def test_get_invalid_name_files():
    shutil.copyfile(
        "tests/resources/test_datastore_input/MY_DATASET.tar",
        "tests/resources/test_datastore_input/DATASET_WITH_INVAL&D_NAM+E.tar",
    )
    response = client.get("/importable-datasets")
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


def test_delete_importable_datasets_api():
    open(
        "tests/resources/test_datastore_input/DATASET_THAT_SHOULD_BE_DELETED.tar",
        "x",
    )
    response = client.delete(
        "/importable-datasets/DATASET_THAT_SHOULD_BE_DELETED",
    )
    assert response.status_code == 200
    assert not os.path.exists(
        "tests/resources/test_datastore_input/DATASET_THAT_SHOULD_BE_DELETED.tar"
    )


def test_delete_nonexisting_dataset():
    response = client.delete(
        "/importable-datasets/NONEXISTING_DATASET",
    )
    assert response.status_code == 404


def test_delete_invalid_name_dataset():
    response = client.delete("/importable-datasets/INVALID_NAME_DATASET++")
    assert response.status_code == 400
