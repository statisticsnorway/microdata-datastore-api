import pytest
from pydantic import ValidationError

from datastore_api.api.request_models import MetadataQuery
from datastore_api.exceptions.exceptions import RequestValidationException


def test_metadata_query_correct_version():
    query = MetadataQuery(version="1.0.0.0")
    assert query.version == "1.0.0.0"


def test_metadata_query_no_version():
    with pytest.raises(ValidationError) as e:
        MetadataQuery()
    assert "version" in str(e)


def test_metadata_query_invalid_version():
    with pytest.raises(RequestValidationException) as e:
        MetadataQuery(version="1.0.0")
    assert "Version is in incorrect format" in e.value.message["message"]


def test_metadata_query_invalid_version2():
    with pytest.raises(RequestValidationException) as e:
        MetadataQuery(version="1.0.0.0.0")
    assert "Version is in incorrect format" in e.value.message["message"]


def test_metadata_query_invalid_version3():
    with pytest.raises(RequestValidationException) as e:
        MetadataQuery(version="1_0_0")
    assert "Version is in incorrect format" in e.value.message["message"]


def test_metadata_query_draft_version():
    query = MetadataQuery(version="0.0.0.12345")
    assert query.version == "0.0.0.12345"


def test_metadata_query_invalid_names():
    with pytest.raises(ValidationError) as e:
        MetadataQuery(names={"a": "a"}, version="1.0.0.0")
    assert "Input should be a valid string" in str(e)
