{% macro validate_event_freshness_anomalies() %}
    {%- set max_bucket_end = elementary.edr_quote(elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")) %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
    {% set freshness_validation_query %}
        select distinct table_name
        from {{ alerts_relation }}
        where sub_type = 'event_freshness' and detected_at >= {{elementary.edr_cast_as_timestamp(max_bucket_end) }}
    {% endset %}

    {% set results = elementary.result_column_to_list(freshness_validation_query) %}
    {{ assert_lists_contain_same_items(results, ['string_column_anomalies',
                                                 'numeric_column_anomalies',
                                                 'string_column_anomalies_training']) }}
{% endmacro %}
