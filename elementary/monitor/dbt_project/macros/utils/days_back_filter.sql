{#
    Renders the `where` predicate for a `days_back` lookback.

    The default implementation is the shape these queries have always used: wrap the column in a
    timestamp cast and compare a `datediff` against `days_back`.

    On BigQuery that shape cannot prune partitions — a function applied to the partition column
    forces a full scan — so `bigquery__` compares the column directly instead, and optionally adds
    a bound on `partition_column`.

    That second bound looks redundant, and logically it is: it is implied by the first. It exists
    because BigQuery only prunes on the raw partition column. It is needed whenever the column
    being filtered is not the partition column, and whenever it has to be cast (dbt_run_results
    stores `generated_at` and `execute_completed_at` as strings, so a cast is unavoidable there
    and no predicate on them can ever prune).

    The bound is deliberately given a day of slack. `created_at` is stamped by the warehouse at
    insert time while the columns it guards come from the dbt client, so the two are not read from
    the same clock. Pruning is only ever at day granularity, so the slack costs at most one extra
    partition and removes any chance of the guard excluding a row the real predicate wanted.

    Args:
        column: the column to filter on.
        days_back: size of the lookback window, in days.
        partition_column: the table's partition column, when it differs from `column`. Adds the
            pruning bound described above.
        column_is_string: set when `column` is stored as a string and needs casting before
            comparison.
#}

{% macro days_back_filter(column, days_back, partition_column=none, column_is_string=false) %}
    {% do return(
        adapter.dispatch("days_back_filter", "elementary_cli")(
            column, days_back, partition_column, column_is_string
        )
    ) %}
{% endmacro %}

{% macro default__days_back_filter(
    column, days_back, partition_column=none, column_is_string=false
) %}
    {% set predicate %}
        {{ elementary.edr_datediff(
            elementary.edr_cast_as_timestamp(column), elementary.edr_current_timestamp(), 'day'
        ) }} < {{ days_back }}
    {% endset %}
    {% do return(predicate) %}
{% endmacro %}

{% macro bigquery__days_back_filter(
    column, days_back, partition_column=none, column_is_string=false
) %}
    {% set window_start = elementary.edr_timeadd(
        "day", -1 * (days_back | int), elementary.edr_current_timestamp()
    ) %}
    {% set filtered = elementary.edr_cast_as_timestamp(column) if column_is_string else column %}
    {% set predicate %}
        {{ filtered }} > {{ window_start }}
        {% if partition_column and partition_column != column %}
            {# a day of slack, since this column is stamped from a different clock #}
            and {{ partition_column }} > {{ elementary.edr_timeadd(
                "day", -1 * (days_back | int + 1), elementary.edr_current_timestamp()
            ) }}
        {% endif %}
    {% endset %}
    {% do return(predicate) %}
{% endmacro %}
