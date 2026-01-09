from unittest.mock import Mock

import pytest

from datastore_api.common.exceptions import DatastoreExistsException
from datastore_api.domain.datastores import (
    create_new_datastore,
)
from datastore_api.domain.datastores.models import NewDatastore

NEW_DATASTORE = NewDatastore(
    rdn="no.new.testdatastore",
    description="new testdatastore",
    directory="tests/resources/datastores/no-new-testdatastore",
    name="NEW TESTDATASTORE",
    bump_enabled=False,
)

EXISTING_DATASTORE = NewDatastore(
    rdn="no.dev.test",
    description="testdatastore",
    directory="tests/resources/datastores/no_dev_test",
    name="TESTDATASTORE",
    bump_enabled=False,
)


@pytest.fixture
def mock_db_client():
    mock = Mock()
    mock.get_datastores = Mock(side_effect=lambda: ["no.dev.test"])
    mock.insert_new_datastore = Mock(return_value=None)
    return mock


@pytest.fixture
def mock_setup_datastore(monkeypatch):
    mock = Mock()
    monkeypatch.setattr("datastore_api.domain.datastores.setup_datastore", mock)
    return mock


def test_create_new_datastore(mock_db_client, mock_setup_datastore):
    create_new_datastore(NEW_DATASTORE, mock_db_client)
    mock_db_client.insert_new_datastore.assert_called_once()
    mock_setup_datastore.assert_called_once()


def test_create_new_datastore_when_rdn_exists(
    mock_db_client, mock_setup_datastore
):
    with pytest.raises(DatastoreExistsException):
        create_new_datastore(EXISTING_DATASTORE, mock_db_client)
