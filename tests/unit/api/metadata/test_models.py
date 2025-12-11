import pytest
from pydantic import ValidationError

from datastore_api.api.datastores.metadata.models import MetadataQuery


def test_metadata_query_no_version():
    with pytest.raises(ValidationError) as e:
        MetadataQuery()  # type: ignore
    assert "version" in str(e)


def test_metadata_query_invalid_names():
    with pytest.raises(ValidationError) as e:
        MetadataQuery(names={"a": "a"}, version="1.0.0.0")  # type: ignore
    assert "Input should be a valid string" in str(e)
