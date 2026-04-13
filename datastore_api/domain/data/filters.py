from pyarrow import compute, dataset


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


def generate_fixed_filter(
    *,
    population_filter: list | None = None,
    value_filter: list[str] | list[int] | None = None,
) -> dataset.Expression | None:
    pop_expr = generate_population_filter(population_filter)
    val_expr = generate_value_filter(value_filter)

    if pop_expr is not None and val_expr is not None:
        return pop_expr & val_expr
    if pop_expr is not None:
        return pop_expr
    return val_expr


def generate_population_filter(
    population_filter: list | None = None,
) -> dataset.Expression | None:
    return (
        dataset.field("unit_id").isin(population_filter)
        if population_filter
        else None
    )


def _build_wildcard_expression(value: str) -> dataset.Expression:
    if value.count("*") != 1 or len(value) < 2:
        raise ValueError("Only one '*' is allowed in value")
    if not value.endswith("*"):
        raise ValueError("Wildcard '*' must be at the end")
    prefix = value[:-1]
    if not prefix:
        raise ValueError(
            "Wildcard '*' must be preceded by at least one character"
        )
    return compute.starts_with(  # type: ignore[attr-defined]
        dataset.field("value"), prefix
    )


def generate_value_int_filter(
    value_filter: list[int],
) -> dataset.Expression | None:
    return dataset.field("value").isin(value_filter)


def generate_value_string_filter(
    value_filter: list[str],
) -> dataset.Expression | None:
    expression: dataset.Expression | None = None
    valid_values: list[str] = []
    for value in value_filter:
        if "*" in value:
            new_wildcard = _build_wildcard_expression(value)
            expression = (
                new_wildcard
                if expression is None
                else expression | new_wildcard
            )
        else:
            valid_values.append(value)
    if valid_values:
        isin_filter = dataset.field("value").isin(valid_values)
        expression = (
            isin_filter if expression is None else expression | isin_filter
        )
    return expression


def generate_value_filter(
    value_filter: list[str] | list[int] | None = None,
) -> dataset.Expression | None:
    if not value_filter:
        return None
    if isinstance(value_filter[0], str):
        return generate_value_string_filter(value_filter)
    return generate_value_int_filter(value_filter)
