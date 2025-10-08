{% macro validate_column_anomalies() %}
    {%- do validate_string_column_anomalies() -%}
    {%- do validate_numeric_column_anomalies() -%}
    {%- do validate_custom_column_monitors() -%}
    {%- do validate_any_type_column_anomalies() -%}
{% endmacro %}

{% macro validate_string_column_anomalies() %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
    {% set string_column_alerts %}
    select distinct column_name
    from {{ alerts_relation }}
    where status in ('fail', 'warn') and lower(sub_type) = lower(column_name) and upper(table_name) = 'STRING_COLUMN_ANOMALIES'
    {% endset %}
    {% set results = elementary.result_column_to_list(string_column_alerts) %}
    {{ assert_lists_contain_same_items(results, ['min_length', 'max_length', 'average_length', 'missing_count',
                                                 'missing_percent']) }}
{% endmacro %}

{% macro validate_numeric_column_anomalies() %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
    {% set numeric_column_alerts %}
    select distinct column_name
    from {{ alerts_relation }}
    where status in ('fail', 'warn') and lower(sub_type) = lower(column_name)
      and upper(table_name) = 'NUMERIC_COLUMN_ANOMALIES'
    {% endset %}
    {% set results = elementary.result_column_to_list(numeric_column_alerts) %}
    {{ assert_lists_contain_same_items(results, ['min', 'max', 'zero_count', 'zero_percent', 'average',
                                                 'standard_deviation', 'variance', 'sum']) }}
{% endmacro %}

{% macro validate_custom_column_monitors() %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
    {% set query %}
    select distinct sub_type from {{ alerts_relation }}
    where status in ('fail', 'warn') and upper(table_name) = 'COPY_NUMERIC_COLUMN_ANOMALIES'
    {% endset %}
    {% set results = elementary.result_column_to_list(query) %}
    {{ assert_lists_contain_same_items(results, ["zero_count"]) }}
{% endmacro %}

{% macro validate_any_type_column_anomalies() %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
    {% set any_type_column_alerts %}
        select column_name, sub_type
        from {{ alerts_relation }}
        where status in ('fail', 'warn') and upper(table_name) = 'ANY_TYPE_COLUMN_ANOMALIES'
          and column_name is not NULL
        group by 1,2
    {% endset %}
    {% set alert_rows = run_query(any_type_column_alerts) %}
    {% set indexed_columns = {} %}
    {% for row in alert_rows %}
        {% set column_name = row[0] %}
        {% set alert = row[1] %}
        {% if column_name in indexed_columns %}
            {% do indexed_columns[column_name].append(alert) %}
        {% else %}
            {% do indexed_columns.update({column_name: [alert]}) %}
        {% endif %}
    {% endfor %}
    {% set results = [] %}
    {% for column, column_alerts in indexed_columns.items() %}
        {% for alert in column_alerts %}
            {% if alert | lower in column | lower %}
                {% do results.append(column) %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    {{ assert_lists_contain_same_items(results, ['null_count_str',
                                                 'null_percent_str',
                                                 'null_count_float',
                                                 'null_percent_float',
                                                 'null_count_int',
                                                 'null_percent_int',
                                                 'null_count_bool',
                                                 'null_percent_bool']) }}
{% endmacro %}
