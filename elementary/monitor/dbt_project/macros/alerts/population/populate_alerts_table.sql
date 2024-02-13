{% macro populate_alerts_table(disable_samples=false) %}
    {% if execute %}
        {% set alerts_v2_relation = elementary.get_elementary_relation('alerts_v2') %}

        {% set test_alerts = elementary_cli.populate_test_alerts(disable_samples=disable_samples) %}
        {% set model_alerts = elementary_cli.populate_model_alerts() %}
        {% set source_freshness_alerts = elementary_cli.populate_source_freshness_alerts() %}
        
        {% set all_alerts = test_alerts + model_alerts + source_freshness_alerts %}
        {% set backward_already_handled_alert_ids = backward_already_handled_alerts() %}

        {% set unhandled_alerts = [] %}
        {% for alert in all_alerts %}
            {% if alert.get('alert_id') not in backward_already_handled_alert_ids %}
                {% do unhandled_alerts.append(alert) %}
            {% endif %}
        {% endfor %}

        {% do elementary.insert_rows(alerts_v2_relation, unhandled_alerts, on_query_exceed=populate_alerts_on_query_exceed) %}
    {% endif %}
    {% do return('') %}
{% endmacro %}


{% macro backward_already_handled_alerts() %}
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
                union all 
                select alert_id
                from {{ deprecated_alerts_models_relation }}
                where suppression_status != 'pending'
                union all 
                select alert_id
                from {{ deprecated_alerts_source_freshness_relation }}
                where suppression_status != 'pending'
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


{% macro populate_alerts_on_query_exceed(alert_row) %}
    {% set row_max_size = elementary.get_config_var('query_max_size') %}

    {# alert data contains data that could exceed the query size limit #}
    {# We remove the problematic fields to insure the query is in the right size #}
    {% set alert_data = alert_row['data'] %}
    {% set alert_data_dict = fromjson(alert_data) %}
    {% set risky_fields = ['test_rows_sample', 'test_results_query'] %}
    {% for risky_field in risky_fields %}
        {% if (tojson(alert_data_dict[risky_field]) | length) > (row_max_size / 3) %}
            {% do alert_data_dict.update({risky_field: none}) %}            
        {% endif %}
    {% endfor %}
    {% do alert_row.update({'data': tojson(alert_data_dict)}) %}
{% endmacro %}
