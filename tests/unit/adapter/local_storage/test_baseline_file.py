from pathlib import Path

import pytest

from datastore_api.adapter.local_storage import BaselineFile, read_baseline_file
from datastore_api.common.exceptions import StartUpException

TEST_DIR = Path("tests/resources/fixtures/adapter/local_storage")
VALID_BASELINE_FILE = TEST_DIR / "valid_baseline.json"
INVALID_BASELINE_FILE = TEST_DIR / "invalid_baseline.json"


def test_get_baseline_file():
    assert isinstance(read_baseline_file(VALID_BASELINE_FILE), BaselineFile)


def test_get_invalid_baseline_file():
    with pytest.raises(StartUpException):
        read_baseline_file(INVALID_BASELINE_FILE)
