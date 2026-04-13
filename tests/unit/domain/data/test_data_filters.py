import pytest

from datastore_api.domain.data.filters import (
    generate_fixed_filter,
    generate_population_filter,
    generate_time_filter,
    generate_time_period_filter,
    generate_value_filter,
)

POPULATION = [1, 2, 3, 4]
INT_VALUES = [1, 2, 3, 4]
STR_VALUES = ["AB", "AC"]
WILDCARD_VALUES = ["AB", "B*", "C*"]

STR_POP_FILTER = (
    "is_in(unit_id, {value_set=int64:"
    "[\n  1,\n  2,\n  3,\n  4\n],"
    " null_matching_behavior=MATCH})"
)

STR_VALUE_FILTER = (
    "is_in(value, {value_set=string:"
    '[\n  "AB",\n  "AC"\n],'
    " null_matching_behavior=MATCH})"
)
INT_VALUE_FILTER = (
    "is_in(value, {value_set=int64:"
    "[\n  1,\n  2,\n  3,\n  4\n],"
    " null_matching_behavior=MATCH})"
)


def test_generate_population_filter():
    actual = generate_population_filter(None)
    assert actual is None
    actual = generate_population_filter(POPULATION)
    assert STR_POP_FILTER in str(actual)


def test_generate_value_filter():
    assert generate_value_filter(value_filter=None) is None

    actual = generate_value_filter(value_filter=INT_VALUES)
    assert INT_VALUE_FILTER in str(actual)

    actual = generate_value_filter(value_filter=STR_VALUES)
    assert STR_VALUE_FILTER in str(actual)

    actual = generate_value_filter(value_filter=["B*"])
    assert "starts_with(value" in str(actual)
    assert "B" in str(actual)

    actual = generate_value_filter(value_filter=WILDCARD_VALUES)
    expr_str = str(actual)
    assert "starts_with" in expr_str
    assert "is_in" in expr_str

    with pytest.raises(ValueError):
        generate_value_filter(value_filter=["*"])

    with pytest.raises(ValueError):
        generate_value_filter(value_filter=["A*B"])


def test_generate_fixed_filter():
    assert (
        generate_fixed_filter(population_filter=None, value_filter=None) is None
    )

    actual = generate_fixed_filter(
        population_filter=POPULATION, value_filter=None
    )
    assert STR_POP_FILTER in str(actual)

    actual = generate_fixed_filter(
        population_filter=None, value_filter=STR_VALUES
    )
    assert STR_VALUE_FILTER in str(actual)

    actual = generate_fixed_filter(
        population_filter=POPULATION, value_filter=STR_VALUES
    )
    expr_str = str(actual)
    assert "unit_id" in expr_str
    assert "value" in expr_str


def test_generate_time_filter():
    str_time_filters = (
        "(start_epoch_days <= 18000)",
        "invert(is_valid(stop_epoch_days)",
        "(start_epoch_days <= 18000)",
        "(stop_epoch_days >= 18000)",
    )
    actual = generate_time_filter(18000, None)
    for str_filter in str_time_filters:
        assert str_filter in str(actual)
    actual = generate_time_filter(18000, POPULATION)
    for str_filter in str_time_filters:
        assert str_filter in str(actual)
    assert STR_POP_FILTER in str(actual)


def test_generate_period_filter():
    str_time_period_filter = (
        "(start_epoch_days <= 18000)",
        "invert(is_valid(stop_epoch_days)",
        "(start_epoch_days <= 18000)",
        "(stop_epoch_days >= 18000)",
        "(start_epoch_days >= 18000)",
        "(start_epoch_days <= 18250)",
        "(start_epoch_days > 18000)",
        "(stop_epoch_days <= 18250)",
    )
    actual = generate_time_period_filter(18000, 18250, None)
    for str_filter in str_time_period_filter:
        assert str_filter in str(actual)
    assert STR_POP_FILTER not in str(actual)
    actual = generate_time_period_filter(18000, 18250, POPULATION)
    for str_filter in str_time_period_filter:
        assert str_filter in str(actual)
    assert STR_POP_FILTER in str(actual)
