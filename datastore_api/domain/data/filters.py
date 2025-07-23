from typing import Union

from pyarrow import dataset


def generate_time_period_filter(
    start: int, stop: int, population_filter: list | None = None
) -> dataset.Expression:
    stop_missing = ~dataset.field("stop_epoch_days").is_valid()
    start_epoch_le_start = dataset.field("start_epoch_days") <= start
    start_epoch_ge_start = dataset.field("start_epoch_days") >= start
    start_epoch_le_stop = dataset.field("start_epoch_days") <= stop
    start_epoch_g_start = dataset.field("start_epoch_days") > start
    stop_epoch_ge_start = dataset.field("stop_epoch_days") >= start
    stop_epoch_le_stop = dataset.field("stop_epoch_days") <= stop

    find_by_time_period_filter = (
        (start_epoch_le_start & stop_missing)
        | (start_epoch_le_start & stop_epoch_ge_start)
        | (start_epoch_ge_start & start_epoch_le_stop)
        | (start_epoch_g_start & stop_epoch_le_stop)
    )
    if population_filter:
        population = generate_population_filter(population_filter)
        find_by_time_period_filter = population & find_by_time_period_filter
    return find_by_time_period_filter


def generate_time_filter(
    date: int, population_filter: list | None = None
) -> dataset.Expression:
    stop_missing = ~dataset.field("stop_epoch_days").is_valid()
    start_epoch_le_date = dataset.field("start_epoch_days") <= date
    stop_epoch_ge_date = dataset.field("stop_epoch_days") >= date

    find_by_time_filter = (start_epoch_le_date & stop_missing) | (
        start_epoch_le_date & stop_epoch_ge_date
    )
    if population_filter:
        population = generate_population_filter(population_filter)
        find_by_time_filter = population & find_by_time_filter
    return find_by_time_filter


def generate_population_filter(
    population_filter: list | None = None,
) -> Union[dataset.Expression, None]:
    return (
        dataset.field("unit_id").isin(population_filter)
        if population_filter
        else None
    )
