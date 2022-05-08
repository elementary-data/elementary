{% macro get_new_alerts(days_back, results_sample_limit = 5) %}
    -- depends_on: {{ ref('alerts') }}
    {% set current_date = dbt_utils.date_trunc('day', dbt_utils.current_timestamp()) %}
    {% set select_new_alerts_query %}
        SELECT alert_id, detected_at, database_name, schema_name, table_name, column_name, alert_type, sub_type,
               alert_description, owners, tags, alert_results_query, other, test_name, test_params, severity, status
        FROM {{ ref('alerts') }}
        WHERE alert_sent = FALSE and detected_at >= {{ get_alerts_time_limit(days_back) }}
    {% endset %}
    {% set results = run_query(select_new_alerts_query) %}
    {% set new_alerts = [] %}
    {% for result in results %}
        {% set alert_results_query = result[11] %}
        {% set alert_type = result[6] %}
        {% set alert_results = none %}
        {% set serializable_test_results = none %}

        {% if alert_results_query %}
            {% set alert_results_query_with_limit = alert_results_query ~ ' limit ' ~ results_sample_limit %}
            {% set test_results = run_query(alert_results_query_with_limit) %}
            {% set serializable_test_results = agate_to_json(test_results) %}
        {% endif %}

        {% set new_alert_dict = {'alert_id': result[0],
                                 'detected_at': result[1].isoformat(),
                                 'database_name': result[2],
                                 'schema_name': result[3],
                                 'table_name': result[4],
                                 'column_name': result[5],
                                 'alert_type': alert_type,
                                 'sub_type': result[7],
                                 'alert_description': result[8],
                                 'owners': result[9],
                                 'tags': result[10],
                                 'alert_results_query': alert_results_query,
                                 'alert_results': serializable_test_results,
                                 'other': result[12],
                                 'test_name': result[13],
                                 'test_params': result[14],
                                 'severity': result[15],
                                 'status': result[16]} %}
        {% set new_alert_json = tojson(new_alert_dict) %}
        {% do elementary.edr_log(new_alert_json) %}
    {% endfor %}
{% endmacro %}

{% macro agate_to_json(agate_table) %}
    {% set column_types = agate_table.column_types %}
    {% set serializable_rows = [] %}
    {% for agate_row in agate_table.rows %}
        {% set serializable_row = {} %}
        {% for col_name, col_value in agate_row.items() %}
            {% set serializable_col_value = column_types[loop.index0].jsonify(col_value) %}
            {% do serializable_row.update({col_name: serializable_col_value}) %}
        {% endfor %}
        {% do serializable_rows.append(serializable_row) %}
    {% endfor %}
    {{ return(tojson(serializable_rows)) }}
{% endmacro %}