import pytest
from unittest.mock import Mock
from datastore_api.adapter import db
from datastore_api.adapter.db.models import (
    Job,
    UserInfo,
    JobStatus,
    JobParameters,
    Target,
)
from fastapi.testclient import TestClient

from datastore_api.main import app


JOB_ID = "123-123-123-123"
USER_INFO_DICT = {
    "userId": "123-123-123",
    "firstName": "Data",
    "lastName": "Admin",
}
USER_INFO = UserInfo.model_validate(USER_INFO_DICT)
TARGET_LIST = [
    Target(
        name="MY_DATASET",
        last_updated_at="2022-05-18T11:40:22.519222",
        status=JobStatus("completed"),
        action=["ADD"],
        last_updated_by=USER_INFO,
    ),
    Target(
        name="OTHER_DATASET",
        last_updated_at="2022-05-18T11:40:22.519222",
        status=JobStatus("completed"),
        action=["SET_STATUS", "PENDING_RELEASE"],
        last_updated_by=USER_INFO,
    ),
]
JOB_LIST = [
    Job(
        job_id="123-123-123-123",
        status=JobStatus("completed"),
        parameters=JobParameters.model_validate(
            {"target": "MY_DATASET", "operation": "ADD"}
        ),
        created_at="2022-05-18T11:40:22.519222",
        created_by=USER_INFO,
    ),
    Job(
        job_id="123-123-123-123",
        status=JobStatus("completed"),
        parameters=JobParameters.model_validate(
            {"target": "OTHER_DATASET", "operation": "ADD"}
        ),
        created_at="2022-05-18T11:40:22.519222",
        created_by=USER_INFO,
    ),
]


@pytest.fixture
def mock_db_client():
    mock = Mock()
    mock.get_targets.return_value = TARGET_LIST
    mock.get_jobs_for_target.return_value = JOB_LIST
    return mock


@pytest.fixture
def client(mock_db_client):
    app.dependency_overrides[db.get_database_client] = lambda: mock_db_client
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_get_targets(client, mock_db_client):
    response = client.get("/targets")
    assert response.json() == [
        target.model_dump(exclude_none=True, by_alias=True)
        for target in TARGET_LIST
    ]
    assert response.status_code == 200
    mock_db_client.get_targets.assert_called_once()


def test_get_target(client, mock_db_client):
    response = client.get("/targets/MY_DATASET/jobs")
    mock_db_client.get_jobs_for_target.assert_called_once()
    mock_db_client.get_jobs_for_target.assert_called_with("MY_DATASET")
    assert response.status_code == 200
    assert response.json() == [
        job.model_dump(exclude_none=True, by_alias=True) for job in JOB_LIST
    ]
