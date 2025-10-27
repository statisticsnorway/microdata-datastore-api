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
from datastore_api.main import app

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
        datastore_rdn="no.ssb.test",
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
        datastore_rdn="no.ssb.test",
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
    rdn="no.ssb.test",
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
    mock.new_job.return_value = JOB_LIST[0]
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


def test_get_jobs_for_datastore(client, mock_db_client):
    response = client.get(
        "datastores/no.ssb.test/jobs?status=completed&operation=ADD,CHANGE,PATCH_METADATA"
    )
    assert response.json() == [
        job.model_dump(exclude_none=True, by_alias=True) for job in JOB_LIST
    ]
    assert response.status_code == 200
    mock_db_client.get_jobs.assert_called_once()



def test_new_job(client, mock_db_client, mock_auth_client):
    response = client.post("datastores/no.ssb.test/jobs", json=NEW_JOB_REQUEST)
    assert mock_db_client.new_job.call_count == 2
    assert mock_db_client.update_target.call_count == 2
    mock_auth_client.authorize_data_administrator.assert_called_once()
    assert response.status_code == 200
    assert response.json() == [
        {"msg": "CREATED", "status": "queued", "job_id": JOB_ID},
        {"msg": "CREATED", "status": "queued", "job_id": JOB_ID},
    ]
