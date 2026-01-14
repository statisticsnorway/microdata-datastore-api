import shutil
from pathlib import Path

import pytest

from datastore_api.adapter.local_storage import setup_datastore
from datastore_api.common.exceptions import DatastorePathExistsException

RDN = "no.new.testdatastore"
DESCRIPTION = "new testdatastore"
DIRECTORY = "tests/resources/datastores/no-new-testdatastore"
NAME = "NEW TESTDATASTORE"


@pytest.fixture
def cleanup_datastores():
    yield
    shutil.rmtree("tests/resources/datastores", ignore_errors=True)


def test_setup_datastore(cleanup_datastores):
    setup_datastore(
        rdn=RDN, name=NAME, directory=DIRECTORY, description=DESCRIPTION
    )
    root_path = Path(DIRECTORY)
    expected_dirs = [
        root_path,
        root_path / "data",
        root_path / "datastore",
        root_path / "vault",
        root_path.with_name(root_path.name + "_input"),
        root_path.with_name(root_path.name + "_working"),
    ]

    for path in expected_dirs:
        assert path.exists()

    expected_files = {
        "datastore_versions.json",
        "draft_version.json",
        "metadata_all__DRAFT.json",
    }
    actual_files = {
        file.name
        for file in (root_path / "datastore").iterdir()
        if file.is_file()
    }

    assert expected_files == actual_files


def test_setup_datastore_on_existing_path(cleanup_datastores):
    datastore_dir = Path(DIRECTORY)
    datastore_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(DatastorePathExistsException):
        setup_datastore(
            rdn=RDN, name=NAME, directory=DIRECTORY, description=DESCRIPTION
        )
