{% macro get_new_alerts() %}
    -- depends_on: {{ ref('alerts') }}
    {% set current_date = dbt_utils.date_trunc('day', dbt_utils.current_timestamp()) %}
    {% set select_new_alerts_query %}
        SELECT alert_id, detected_at, database_name, schema_name, table_name, column_name, alert_type, sub_type,
               alert_description
        FROM {{ ref('alerts') }}
        WHERE alert_sent = FALSE and detected_at >= {{ get_alerts_time_limit() }}
    {% endset %}
    {% set results = run_query(select_new_alerts_query) %}
    {% set new_alerts = [] %}
    {% for result in results %}
        {% set new_alert_dict = {'alert_id': result[0],
                                 'detected_at': result[1].isoformat(),
                                 'database_name': result[2],
                                 'schema_name': result[3],
                                 'table_name': result[4],
                                 'column_name': result[5],
                                 'alert_type': result[6],
                                 'sub_type': result[7],
                                 'alert_description': result[8]} %}
        {% set new_alert_json = tojson(new_alert_dict) %}
        {% do elementary.edr_log(new_alert_json) %}
    {% endfor %}
{% endmacro %}
