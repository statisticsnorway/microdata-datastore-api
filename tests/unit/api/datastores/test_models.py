import pytest

from datastore_api.api.datastores import NewDatastoreRequest


def test_create_new_datastore_with_invalid_request():
    with pytest.raises(ValueError):
        NewDatastoreRequest.validate_rdn("no test.rdn")

    with pytest.raises(ValueError):
        NewDatastoreRequest.validate_name("TESTDATASTORE &")
