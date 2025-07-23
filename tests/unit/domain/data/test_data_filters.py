from datastore_api.domain.data.filters import (
    generate_population_filter,
    generate_time_filter,
    generate_time_period_filter,
)

POPULATION = [1, 2, 3, 4]
STR_POP_FILTER = (
    "is_in(unit_id, {value_set=int64:"
    "[\n  1,\n  2,\n  3,\n  4\n],"
    " null_matching_behavior=MATCH})"
)


def test_generate_population_filter():
    actual = generate_population_filter(None)
    assert actual is None
    actual = generate_population_filter(POPULATION)
    assert STR_POP_FILTER in str(actual)


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
