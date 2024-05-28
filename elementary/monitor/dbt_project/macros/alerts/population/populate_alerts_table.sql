{% macro populate_alerts_table(days_back=1, disable_samples=false) %}
    {% if execute %}
        {% set alerts_v2_relation = elementary.get_elementary_relation('alerts_v2') %}

        {% set test_alerts = elementary_cli.populate_test_alerts(days_back=days_back, disable_samples=disable_samples) %}
        {% set model_alerts = elementary_cli.populate_model_alerts(days_back=days_back) %}
        {% set source_freshness_alerts = elementary_cli.populate_source_freshness_alerts(days_back=days_back) %}
        
        {% set all_alerts = test_alerts + model_alerts + source_freshness_alerts %}
        {% set backward_already_handled_alert_ids = backward_already_handled_alerts(days_back=days_back) %}

        {% set unhandled_alerts = [] %}
        {% for alert in all_alerts %}
            {% if alert.get('alert_id') not in backward_already_handled_alert_ids %}
                {% do elementary_cli.handle_exceeding_limit_alert_data(alert) %}
                {% do unhandled_alerts.append(alert) %}
            {% endif %}
        {% endfor %}
        {% do elementary.insert_rows(alerts_v2_relation, unhandled_alerts, on_query_exceed=elementary_cli.handle_exceeding_limit_alert_data) %}
    {% endif %}
    {% do return('') %}
{% endmacro %}


{% macro backward_already_handled_alerts(days_back=1) %}
    {% set deprecated_alerts_relation = ref('elementary_cli', 'alerts') %}
    {% set deprecated_alerts_models_relation = ref('elementary_cli', 'alerts_models') %}
    {% set deprecated_alerts_source_freshness_relation = ref('elementary_cli', 'alerts_source_freshness') %}

    {% set alert_ids = [] %}

    {% if load_relation(deprecated_alerts_relation) is not none %}
        {% set handled_alerts_query %}
            with alerts_ids as (
                select alert_id
                from {{ deprecated_alerts_relation }}
                where suppression_status != 'pending'
                and {{ elementary.edr_cast_as_timestamp('detected_at') }} > {{ elementary.edr_timeadd('day', -1 * days_back, elementary.edr_current_timestamp()) }}
                union all 
                select alert_id
                from {{ deprecated_alerts_models_relation }}
                where suppression_status != 'pending'
                and {{ elementary.edr_cast_as_timestamp('detected_at') }} > {{ elementary.edr_timeadd('day', -1 * days_back, elementary.edr_current_timestamp()) }}
                union all 
                select alert_id
                from {{ deprecated_alerts_source_freshness_relation }}
                where suppression_status != 'pending'
                and {{ elementary.edr_cast_as_timestamp('detected_at') }} > {{ elementary.edr_timeadd('day', -1 * days_back, elementary.edr_current_timestamp()) }}
            )

            select *
            from alerts_ids
        {% endset %}

        {% set handled_alert_ids = elementary.agate_to_dicts(run_query(handled_alerts_query)) %}
        {% for alert_id in handled_alert_ids %}
            {% do alert_ids.append(alert_id["alert_id"]) %}
        {% endfor %}
    {% endif %}

    {% do return(alert_ids) %}
{% endmacro %}


{% macro handle_exceeding_limit_alert_data(alert_row) %}
    {% set row_max_size = elementary.get_config_var('query_max_size') %}
    {% set column_max_size = elementary.get_column_size() %}

    {# alert data contains data that could exceed the query size limit #}
    {# We remove the problematic fields to insure the query is in the right size #}
    {% set alert_data = alert_row['data'] %}
    {% set alert_data_dict = fromjson(alert_data) %}
    {% set risky_fields = ['test_rows_sample', 'test_results_query'] %}
    {% for risky_field in risky_fields %}
        {% set field_length = tojson(alert_data_dict.get(risky_field, {})) | length %}
        {% set exceeding_row_size = field_length > (row_max_size / 3) %}
        {# For some DWH there is no column size limitation #}
        {% set exceeding_column_size = column_max_size and field_length > (column_max_size / 3) %}
        {% if exceeding_row_size or exceeding_column_size %}
            {% do alert_data_dict.update({risky_field: none}) %}            
        {% endif %}
    {% endfor %}
    {% do alert_row.update({'data': tojson(alert_data_dict)}) %}
{% endmacro %}
