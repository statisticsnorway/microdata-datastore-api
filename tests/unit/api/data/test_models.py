from typing import List

import pytest

from datastore_api.api.data.models import (
    InputTimeQuery,
    InputTimePeriodQuery,
    InputFixedQuery,
    InputQuery,
)
from datastore_api.common.models import Version


def test_create_and_validate_minimal_input_time_period_query():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "startDate": 1964,
        "stopDate": 2056,
    }
    InputTimePeriodQuery.model_validate(data)


def test_create_and_validate_full_input_time_period_query():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "startDate": 1964,
        "stopDate": 2056,
        "population": [1, 2, 3],
        "includeAttributes": True,
    }
    actual = InputTimePeriodQuery.model_validate(data)
    assert actual.dataStructureName == "DATASET_NAME"
    assert actual.version == Version.from_str("1.0.0.0")
    assert actual.startDate == 1964
    assert actual.stopDate == 2056
    assert isinstance(actual.population, List)
    assert actual.population == [1, 2, 3]
    assert actual.includeAttributes is True


def test_no_population_type_coercion():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "startDate": 1964,
        "stopDate": 2056,
        "population": [1, 2, 3],
        "includeAttributes": True,
    }
    actual = InputTimePeriodQuery.model_validate(data)
    assert actual.population == data["population"]
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "startDate": 1964,
        "stopDate": 2056,
        "population": ["1", "2", "3"],
        "includeAttributes": True,
    }
    actual = InputTimePeriodQuery.model_validate(data)
    assert actual.population == data["population"]


def test_create_and_validate_input_time_period_query_with_error():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "startDate": 1964,
    }
    with pytest.raises(ValueError):
        InputTimePeriodQuery.model_validate(data)


def test_create_and_validate_minimal_input_time_query():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "date": 1964,
    }
    InputTimeQuery.model_validate(data)


def test_create_and_validate_full_input_time_query():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "date": 1964,
        "population": [1, 2, 3],
        "includeAttributes": True,
    }
    InputTimeQuery.model_validate(data)


def test_create_and_validate_input_time_query_with_error():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.X",
        "date": 1964,
    }
    with pytest.raises(ValueError):
        InputTimeQuery.model_validate(data)


def test_create_and_validate_minimal_input_fixed_query():
    data = {"dataStructureName": "DATASET_NAME", "version": "1.0.0.0"}
    InputFixedQuery.model_validate(data)


def test_create_and_validate_full_input_fixed_query():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "population": [1, 2, 3],
        "includeAttributes": True,
    }
    InputFixedQuery.model_validate(data)


def test_create_and_validate_input_fixed_query_with_error():
    data = {"dataStructureName": "DATASET_NAME", "version": "1.0.0.X"}
    with pytest.raises(ValueError):
        InputFixedQuery.model_validate(data)


def test_population_to_string():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "population": [1, 2, 3],
    }
    actual: InputQuery = InputQuery.model_validate(data)
    assert (
        str(actual) == "dataStructureName='DATASET_NAME' "
        "version='1.0.0.0' "
        "population='<length: 3>' "
        "includeAttributes=False"
    )
    assert actual.population == data["population"]


def test_population_to_string_input_time_query():
    data = {
        "dataStructureName": "DATASET_NAME",
        "version": "1.0.0.0",
        "population": [1, 2, 3],
        "date": 1900,
    }
    actual: InputTimeQuery = InputTimeQuery.model_validate(data)
    assert (
        str(actual) == "dataStructureName='DATASET_NAME' "
        "version='1.0.0.0' "
        "population='<length: 3>' "
        "includeAttributes=False "
        "date=1900"
    )
    assert actual.population == data["population"]
