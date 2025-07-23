import os
import json
import sqlite3
from datetime import datetime

import pytest
from pytest_mock import MockFixture

from datastore_api.adapter.db.sqlite import (
    JobAlreadyCompleteException,
    NotFoundException,
    SqliteDbClient,
    JobExistsException,
)
from datastore_api.adapter.db.models import (
    Job,
    JobStatus,
    Operation,
    UserInfo,
    DataStructureUpdate,
    DatastoreVersion,
    JobParameters,
    Target,
)
from datastore_api.api.jobs.models import NewJobRequest


sqlite_file = "test.db"
sqlite_client = SqliteDbClient(f"sqlite://{sqlite_file}")

USER_INFO_DICT = {
    "userId": "123-123-123",
    "firstName": "Data",
    "lastName": "Admin",
}
USER_INFO = UserInfo.model_validate(USER_INFO_DICT)
JOB = {
    "status": "completed",
    "parameters": {"operation": "ADD", "target": "MY_DATASET"},
    "created_by": USER_INFO_DICT,
    "logs": [
        {"at": datetime.now(), "message": "example log"},
        {"at": datetime.now(), "message": "other example"},
    ],
    "created_at": datetime.now(),
}
JOB2 = {
    "status": "queued",
    "parameters": {"operation": "ADD", "target": "MY_OTHER_DATASET"},
    "created_by": USER_INFO_DICT,
    "logs": [],
    "created_at": datetime.now(),
}
TARGET_LIST = [
    Target(
        name="MY_DATASET",
        last_updated_at="2022-05-18T11:40:22.519222",
        status=JobStatus("completed"),
        action=["SET_STATUS", "PENDING_RELEASE"],
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
TARGET_UPDATE_JOB = Job(
    job_id="123-123-123-123",
    status=JobStatus("queued"),
    parameters=JobParameters.model_validate(
        {"target": "MY_DATASET", "operation": "ADD"}
    ),
    created_at="2022-05-18T11:40:22.519222",
    created_by=USER_INFO,
)
NEW_TARGET_JOB = Job(
    job_id="123-123-123-123",
    status=JobStatus("queued"),
    parameters=JobParameters.model_validate(
        {"target": "NEW_DATASET", "operation": "ADD"}
    ),
    created_at="2022-05-18T11:40:22.519222",
    created_by=USER_INFO,
)
BUMP_JOB = Job(
    job_id="bump-bump-bump-bump",
    status=JobStatus("completed"),
    created_at="2022-05-18T11:40:22.519222",
    created_by=USER_INFO,
    parameters=JobParameters(
        bump_from_version="1.0.0",
        bump_to_version="2.0.0",
        operation=Operation.BUMP,
        target="DATASTORE",
        description="Updates",
        bump_manifesto=DatastoreVersion(
            version="0.0.0.123123",
            description="Draft version",
            release_time=123123,
            language_code="no",
            update_type="MAJOR",
            data_structure_updates=[
                DataStructureUpdate(
                    name="MY_DATASET",
                    description="Update",
                    operation="PATCH_METADATA",
                    release_status="PENDING_RELEASE",
                ),
                DataStructureUpdate(
                    name="FRESH_DATASET",
                    description="Update",
                    operation="REMOVE",
                    release_status="PENDING_DELETE",
                ),
                DataStructureUpdate(
                    name="FRESH_DATASET2",
                    description="Update",
                    operation=Operation.ADD,
                    release_status="PENDING_RELEASE",
                ),
                DataStructureUpdate(
                    name="NOT_BUMPED_DATASET",
                    description="Update",
                    operation=Operation.ADD,
                    release_status="DRAFT",
                ),
            ],
        ),
    ),
)


def teardown_function():
    os.remove(sqlite_file)


def setup_function():
    global sqlite_client
    sqlite_client = SqliteDbClient(f"sqlite://{sqlite_file}")
    conn = sqlite3.connect(sqlite_file)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO datastore (rdn, description, directory, name)
        VALUES (?, ?, ?, ?)
        """,
        (
            "no.ssb.test",
            "unit test datastore",
            "test/resources/test_datastore",
            "SSB test datastore",
        ),
    )
    cursor.execute(
        """
        INSERT INTO job (target, datastore_id, status, created_at, created_by, parameters)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            JOB["parameters"]["target"],
            1,
            JOB["status"],
            JOB["created_at"],
            json.dumps(JOB["created_by"]),
            json.dumps(JOB["parameters"]),
        ),
    )
    job_id = cursor.lastrowid
    for log in JOB.get("logs", []):
        cursor.execute(
            """
            INSERT INTO job_log (job_id, msg, at)
            VALUES (?, ?, ?)
        """,
            (
                job_id,
                log["message"],
                log["at"],
            ),
        )
    cursor.execute(
        """
        INSERT INTO job (target, datastore_id, status, created_at, created_by, parameters)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        (
            JOB2["parameters"]["target"],
            1,
            JOB2["status"],
            JOB2["created_at"],
            json.dumps(JOB2["created_by"]),
            json.dumps(JOB2["parameters"]),
        ),
    )
    for target in TARGET_LIST:
        cursor.execute(
            """
            INSERT INTO target (name, datastore_id, last_updated_at, status, action, last_updated_by)
            VALUES (?,  ?, ?, ?, ?, ?)
            """,
            (
                target.name,
                1,
                target.last_updated_at,
                str(target.status),
                ",".join(target.action),
                json.dumps(USER_INFO_DICT),
            ),
        )
    conn.commit()


def test_get_job():
    job = sqlite_client.get_job(1)
    assert isinstance(job, Job)
    assert job.log
    job = sqlite_client.get_job(2)
    assert isinstance(job, Job)
    assert not job.log


def test_get_jobs():
    jobs = sqlite_client.get_jobs(
        status=None, operations=None, ignore_completed=False
    )
    assert len(jobs) == 2
    jobs = sqlite_client.get_jobs(
        status=JobStatus("queued"),
        operations=[Operation.ADD],
        ignore_completed=True,
    )
    assert len(jobs) == 1


def test_get_jobs_for_target():
    jobs = sqlite_client.get_jobs_for_target("MY_DATASET")
    assert len(jobs) == 1


def test_new_job():
    job = sqlite_client.new_job(
        NewJobRequest(
            operation=Operation.ADD, target="NEW_DATASET"
        ).generate_job_from_request("", UserInfo(**USER_INFO_DICT)),
    )
    assert job
    jobs = sqlite_client.get_jobs_for_target("NEW_DATASET")
    assert len(jobs) == 1
    with pytest.raises(JobExistsException):
        sqlite_client.new_job(
            NewJobRequest(
                operation=Operation.ADD, target="NEW_DATASET"
            ).generate_job_from_request("", UserInfo(**USER_INFO_DICT))
        )


def test_update_job():
    existing_job = sqlite_client.get_job(2)
    assert existing_job.status == "queued"
    updated_job = sqlite_client.update_job(
        "2", status=JobStatus("validating"), description=None, log=None
    )
    assert updated_job
    assert updated_job.status == "validating"
    assert (updated_job.log or [])[0].message == "Set status: validating"
    assert updated_job == sqlite_client.get_job(2)
    updated_job = sqlite_client.update_job(
        "2",
        status=JobStatus("pseudonymizing"),
        description=None,
        log="even newer update log",
    )
    assert updated_job
    assert updated_job.status == "pseudonymizing"
    assert (updated_job.log or [])[1].message == "Set status: pseudonymizing"
    assert (updated_job.log or [])[2].message == "even newer update log"
    assert updated_job == sqlite_client.get_job(2)

    with pytest.raises(NotFoundException):
        sqlite_client.update_job(
            "33", status=JobStatus("validating"), description=None, log=None
        )


def test_new_job_different_created_at():
    job1 = NewJobRequest(
        operation=Operation.ADD, target="NEW_DATASET"
    ).generate_job_from_request("abc", UserInfo(**USER_INFO_DICT))

    job2 = NewJobRequest(
        operation=Operation.ADD, target="NEW_DATASET"
    ).generate_job_from_request("def", UserInfo(**USER_INFO_DICT))
    assert job1.created_at != job2.created_at


def test_update_job_completed():
    existing_job = sqlite_client.get_job(2)
    assert existing_job.status == "queued"
    updated_job = sqlite_client.update_job(
        "2", status=JobStatus("completed"), description=None, log=None
    )
    assert updated_job
    assert updated_job.status == "completed"

    assert (updated_job.log or [])[0] is not None
    assert (updated_job.log or [])[0].message == "Set status: completed"
    assert updated_job == sqlite_client.get_job(2)

    with pytest.raises(JobAlreadyCompleteException):
        sqlite_client.update_job(
            "1", status=JobStatus("completed"), description=None, log=None
        )


def test_update_job_failed():
    existing_job = sqlite_client.get_job(2)
    assert existing_job.status == "queued"
    updated_job = sqlite_client.update_job(
        "2", status=JobStatus("failed"), description=None, log=None
    )
    assert updated_job
    assert updated_job.status == "failed"
    assert (updated_job.log or [])[0].message == "Set status: failed"
    assert updated_job == sqlite_client.get_job(2)

    with pytest.raises(JobAlreadyCompleteException):
        sqlite_client.update_job(
            "1", status=JobStatus("failed"), description=None, log=None
        )


def test_initialize_after_get_maintenance_latest_status(mocker: MockFixture):
    spy = mocker.spy(sqlite_client, "initialize_maintenance")
    latest = sqlite_client.get_latest_maintenance_status()
    assert spy.call_count == 1
    assert latest


def test_initialize_after_get_maintenance_history(mocker: MockFixture):
    spy = mocker.spy(sqlite_client, "initialize_maintenance")
    history = sqlite_client.get_maintenance_history()
    assert spy.call_count == 1
    assert history
    assert len(history) == 1


def test_set_and_get_maintenance_status():
    maintenance_status = sqlite_client.set_maintenance_status(
        msg="test",
        paused=True,
    )
    assert maintenance_status
    assert sqlite_client.get_latest_maintenance_status() == maintenance_status
    assert sqlite_client.get_maintenance_history() == [maintenance_status]

    new_maintenance_status = sqlite_client.set_maintenance_status(
        msg="test2",
        paused=False,
    )
    assert new_maintenance_status
    assert (
        sqlite_client.get_latest_maintenance_status() == new_maintenance_status
    )
    assert sqlite_client.get_maintenance_history() == [
        new_maintenance_status,
        maintenance_status,
    ]


def test_get_targets():
    targets = sqlite_client.get_targets()
    target_names = [target.name for target in targets]
    assert "MY_DATASET" in target_names
    assert "OTHER_DATASET" in target_names


def test_update_target():
    sqlite_client.update_target(TARGET_UPDATE_JOB)
    targets = sqlite_client.get_targets()
    assert len(targets) == 2
    target = targets[0]
    assert target.action == ["ADD"]
    assert target.status == "queued"

    sqlite_client.update_target(NEW_TARGET_JOB)
    targets = sqlite_client.get_targets()
    assert len(targets) == 3
    target_names = [target.name for target in targets]
    assert "NEW_DATASET" in target_names
    assert "OTHER_DATASET" in target_names
    assert "MY_DATASET" in target_names


def test_update_targets_bump():
    sqlite_client.update_bump_targets(BUMP_JOB)
    targets = sqlite_client.get_targets()
    assert len(targets) == 4
    target_names = [target.name for target in targets]
    assert "MY_DATASET" in target_names
    assert "OTHER_DATASET" in target_names
    assert "FRESH_DATASET" in target_names
    assert "FRESH_DATASET2" in target_names
    assert all(
        [
            target.action == ["RELEASED", "2.0.0"]
            or target.action == ["REMOVED", "2.0.0"]
            for target in targets
            if target.name != "OTHER_DATASET"
        ]
    )
    assert all(
        [
            target.action == ["SET_STATUS", "PENDING_RELEASE"]
            for target in targets
            if target.name == "OTHER_DATASET"
        ]
    )
