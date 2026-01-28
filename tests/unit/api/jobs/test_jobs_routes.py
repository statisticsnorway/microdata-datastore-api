from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from datastore_api.adapter import auth, db
from datastore_api.adapter.db.models import (
    Datastore,
    Job,
    JobParameters,
    JobStatus,
    UserInfo,
)
from datastore_api.common.exceptions import NotFoundException
from datastore_api.main import app

DATASTORE_RDN = "no.dev.test"
NOT_FOUND_MESSAGE = "Not found"
JOB_ID = "123-123-123-123"
USER_INFO_DICT = {
    "userId": "123-123-123",
    "firstName": "Data",
    "lastName": "Admin",
}
USER_INFO = UserInfo(**USER_INFO_DICT)
JOB_LIST = [
    Job(
        job_id="123-123-123-123",
        status=JobStatus("completed"),
        parameters=JobParameters.model_validate(
            {
                "target": "MY_DATASET",
                "operation": "ADD",
            }
        ),
        created_at="2022-05-18T11:40:22.519222",
        created_by=USER_INFO,
        datastore_rdn=DATASTORE_RDN,
    ),
    Job(
        job_id="123-123-123-123",
        status=JobStatus("completed"),
        parameters=JobParameters.model_validate(
            {
                "target": "OTHER_DATASET",
                "operation": "ADD",
            }
        ),
        created_at="2022-05-18T11:40:22.519222",
        created_by=USER_INFO,
        datastore_rdn=DATASTORE_RDN,
    ),
]
NEW_JOB_REQUEST = {
    "jobs": [
        {"operation": "ADD", "target": "MY_DATASET"},
        {"operation": "CHANGE", "target": "OTHER_DATASET"},
    ]
}
BUMP_JOB_REQUEST = {
    "jobs": [
        {
            "operation": "BUMP",
            "target": "DATASTORE",
            "description": "Bump datastore version",
            "bumpFromVersion": "1.0.0",
            "bumpToVersion": "1.1.0",
            "bumpManifesto": {
                "version": "0.0.0.1634512323",
                "description": "Draft",
                "releaseTime": 1634512323,
                "languageCode": "no",
                "updateType": "MINOR",
                "dataStructureUpdates": [
                    {
                        "name": "MY_DATASET",
                        "description": "Første publisering",
                        "operation": "ADD",
                        "releaseStatus": "PENDING_RELEASE",
                    }
                ],
            },
        }
    ]
}
UPDATE_JOB_REQUEST = {"status": "initiated", "log": "extra logging"}

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
    mock.update_target.return_value = None
    mock.get_job.return_value = JOB_LIST[0]
    mock.get_jobs.return_value = JOB_LIST
    mock.insert_new_job.return_value = JOB_LIST[0]
    mock.update_job.return_value = JOB_LIST[0]
    mock.get_datastore.return_value = DATASTORE
    return mock


@pytest.fixture
def mock_auth_client():
    mock = Mock()
    mock.authorize_data_administrator.return_value = USER_INFO
    return mock


@pytest.fixture
def client(mock_db_client, mock_auth_client):
    app.dependency_overrides[db.get_database_client] = lambda: mock_db_client
    app.dependency_overrides[auth.get_auth_client] = lambda: mock_auth_client
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_get_jobs(client, mock_db_client):
    response = client.get(
        "jobs?status=completed&operation=ADD,CHANGE,PATCH_METADATA"
    )
    assert response.json() == [
        job.model_dump(exclude_none=True, by_alias=True) for job in JOB_LIST
    ]
    assert response.status_code == 200
    mock_db_client.get_jobs.assert_called_once()


def test_get_job(client, mock_db_client):
    response = client.get(f"/jobs/{JOB_ID}")
    mock_db_client.get_job.assert_called_once()
    mock_db_client.get_job.assert_called_with(JOB_ID)
    assert response.status_code == 200
    assert response.json() == JOB_LIST[0].model_dump(
        exclude_none=True, by_alias=True
    )


def test_get_job_not_found(client, mock_db_client):
    mock_db_client.get_job.side_effect = NotFoundException(NOT_FOUND_MESSAGE)
    response = client.get(f"/jobs/{JOB_ID}")
    mock_db_client.get_job.assert_called_once()
    mock_db_client.get_job.assert_called_with(JOB_ID)
    assert response.status_code == 404
    assert response.json() == {"message": NOT_FOUND_MESSAGE}


def test_update_job(client, mock_db_client):
    response = client.put(f"/jobs/{JOB_ID}", json=UPDATE_JOB_REQUEST)
    mock_db_client.update_target.assert_called_once()
    mock_db_client.update_job.assert_called_once()
    mock_db_client.update_job.assert_called_with(
        JOB_ID,
        JobStatus(UPDATE_JOB_REQUEST["status"]),
        None,
        UPDATE_JOB_REQUEST["log"],
    )
    assert response.status_code == 200
    assert response.json() == {"message": f"Updated job with jobId {JOB_ID}"}


def test_update_job_bad_request(client, mock_db_client):
    response = client.put(
        f"/jobs/{JOB_ID}",
        json={"status": "no-such-status"},
    )
    mock_db_client.update_target.assert_not_called()
    assert response.status_code == 400
    assert response.json().get("details") is not None


# -------- RDN ---------
def test_get_jobs_rdn(client, mock_db_client, mock_auth_client):
    response = client.get(
        "/datastores/{DATASTORE_RDN}/jobs?status=completed&operation=ADD,CHANGE,PATCH_METADATA"
    )
    assert response.json() == [
        job.model_dump(exclude_none=True, by_alias=True) for job in JOB_LIST
    ]
    assert response.status_code == 200
    mock_auth_client.authorize_data_administrator.assert_called_once()
    mock_db_client.get_jobs.assert_called_once()


def test_get_job_rdn(client, mock_db_client, mock_auth_client):
    response = client.get(f"/datastores/{DATASTORE_RDN}/jobs/{JOB_ID}")
    mock_db_client.get_job.assert_called_once()
    mock_db_client.get_job.assert_called_with(JOB_ID)
    mock_auth_client.authorize_data_administrator.assert_called_once()
    assert response.status_code == 200
    assert response.json() == JOB_LIST[0].model_dump(
        exclude_none=True, by_alias=True
    )


def test_get_job_not_found_rdn(client, mock_db_client, mock_auth_client):
    mock_db_client.get_job.side_effect = NotFoundException(NOT_FOUND_MESSAGE)
    response = client.get(f"/datastores/{DATASTORE_RDN}/jobs/{JOB_ID}")
    mock_db_client.get_job.assert_called_once()
    mock_db_client.get_job.assert_called_with(JOB_ID)
    mock_auth_client.authorize_data_administrator.assert_called_once()
    assert response.status_code == 404
    assert response.json() == {"message": NOT_FOUND_MESSAGE}


def test_new_job_rdn(client, mock_db_client, mock_auth_client):
    response = client.post(
        "/datastores/{DATASTORE_RDN}/jobs", json=NEW_JOB_REQUEST
    )
    assert mock_db_client.insert_new_job.call_count == 2
    assert mock_db_client.update_target.call_count == 2
    mock_auth_client.authorize_data_administrator.assert_called_once()
    assert response.status_code == 200
    assert response.json() == [
        {"msg": "CREATED", "status": "queued", "job_id": JOB_ID},
        {"msg": "CREATED", "status": "queued", "job_id": JOB_ID},
    ]


def test_update_job_disabled_bump_rdn(client):
    response = client.post(
        "/datastores/{DATASTORE_RDN}/jobs", json=BUMP_JOB_REQUEST
    )
    assert response.status_code == 200
    assert response.json() == [
        {
            "msg": "FAILED: Bumping the datastore is disabled",
            "status": "FAILED",
        },
    ]
