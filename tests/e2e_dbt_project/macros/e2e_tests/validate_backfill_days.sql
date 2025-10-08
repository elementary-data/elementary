{% macro validate_backfill_days() %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
    {% set string_column_alerts %}
    select column_name
    from {{ alerts_relation }}
        where status in ('fail', 'warn') and lower(sub_type) = lower(column_name) and upper(table_name) = 'BACKFILL_DAYS_COLUMN_ANOMALIES'
    {% endset %}
    {% set results = elementary.result_column_to_list(string_column_alerts) %}
    {{ assert_lists_contain_same_items(results, ['min_length']) }}
{% endmacro %}