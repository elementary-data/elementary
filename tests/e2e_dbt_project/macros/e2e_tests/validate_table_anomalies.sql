{% macro validate_table_anomalies() %}
    -- no validation data which means table freshness and volume should alert
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
    {% set freshness_validation_query %}
        select distinct table_name
        from {{ alerts_relation }}
        where status in ('fail', 'warn') and sub_type = 'freshness'
    {% endset %}
    {% set results = elementary.result_column_to_list(freshness_validation_query) %}
    {{ assert_lists_contain_same_items(results, ['string_column_anomalies',
                                                 'numeric_column_anomalies',
                                                 'string_column_anomalies_training']) }}
    {% set row_count_validation_query %}
        select distinct table_name
        from {{ alerts_relation }}
        where status in ('fail', 'warn') and sub_type = 'row_count'
    {% endset %}
    {% set results = elementary.result_column_to_list(row_count_validation_query) %}
    {{ assert_lists_contain_same_items(results, ['users_per_hour_daily_seasonal',
                                                 'users_per_day_weekly_seasonal',
                                                 'any_type_column_anomalies',
                                                 'numeric_column_anomalies',
                                                 'string_column_anomalies_training']) }}

{% endmacro %}
