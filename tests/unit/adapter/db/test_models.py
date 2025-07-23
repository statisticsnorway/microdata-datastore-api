import json

import pytest

from datastore_api.adapter.db.models import Job


RESOURCE_DIR = "tests/resources/models"
with open(f"{RESOURCE_DIR}/valid_jobs.json", encoding="utf-8") as f:
    VALID_JOBS = json.load(f)
with open(f"{RESOURCE_DIR}/invalid_jobs.json", encoding="utf-8") as f:
    INVALID_JOBS = json.load(f)


def test_valid_jobs():
    for job_dict in VALID_JOBS:
        job = Job(**job_dict)
        assert job.model_dump(exclude_none=True, by_alias=True) == {
            "log": [],
            **job_dict,
        }


def test_invalid_jobs():
    for job_dict in INVALID_JOBS:
        with pytest.raises(ValueError):
            Job(**job_dict)
