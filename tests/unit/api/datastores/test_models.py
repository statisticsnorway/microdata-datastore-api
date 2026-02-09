import pytest

from datastore_api.api.datastores.models import NewDatastoreRequest
from datastore_api.domain.datastores.models import NewDatastore

NEW_DATASTORE_REQUEST = NewDatastoreRequest(
    rdn="no.new.testdatastore",
    description="new testdatastore",
    name="NEW TESTDATASTORE",
)

NEW_DATASTORE = NewDatastore(
    rdn="no.new.testdatastore",
    description="new testdatastore",
    directory="tests/resources/datastores/no.new.testdatastore",
    name="NEW TESTDATASTORE",
    bump_enabled=False,
)


def test_create_new_datastore_with_invalid_request():
    with pytest.raises(ValueError):
        NewDatastoreRequest.validate_rdn("no test.rdn")

    with pytest.raises(ValueError):
        NewDatastoreRequest.validate_name("TESTDATASTORE &")


def test_generate_new_datastore_from_request():
    new_datastore = NEW_DATASTORE_REQUEST.generate_new_datastore_from_request()
    assert new_datastore == NEW_DATASTORE
