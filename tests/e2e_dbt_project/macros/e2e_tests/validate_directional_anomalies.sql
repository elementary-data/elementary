{% macro validate_directional_anomalies() %}
    {%- do validate_spike_directional_anomalies() -%}
    {%- do validate_drop_directional_anomalies() -%}
{% endmacro %}

{% macro validate_spike_directional_anomalies() %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
    {# Validating alert for correct direction anomalies #}

    {% set row_count_validation_query %}
        select distinct table_name
        from {{ alerts_relation }}
        where status in ('fail', 'warn') and tags like '%directional_anomalies%' and tags like '%spike%';
    {% endset %}
    {% set results = elementary.result_column_to_list(row_count_validation_query) %}
    -- The result list's purpose is a more readable error messages
    {% set results_list = [] %}
    {% for result in results %}
        {% do results_list.append(result) %}
    {% endfor %}
    {{ assert_lists_contain_same_items(results_list, ['any_type_column_anomalies', 'numeric_column_anomalies']) }}
{% endmacro %}

{% macro validate_drop_directional_anomalies() %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
    {# Validating alert for correct direction anomalies #}

    {% set row_count_validation_query %}
        select distinct table_name
        from {{ alerts_relation }}
        where status in ('fail', 'warn') and tags like '%directional_anomalies%' and tags like '%drop%';
    {% endset %}
    {% set results = elementary.result_column_to_list(row_count_validation_query) %}
    -- The result list's purpose is a more readable error messages
    {% set results_list = [] %}
    {% for result in results %}
        {% do results_list.append(result) %}
    {% endfor %}
    {{ assert_lists_contain_same_items(results_list,  ['any_type_column_anomalies', 'dimension_anomalies']) }}
{% endmacro %}