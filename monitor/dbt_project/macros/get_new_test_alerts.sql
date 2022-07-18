{% macro get_new_test_alerts(days_back, results_sample_limit = 5) %}
    -- depends_on: {{ ref('alerts_tests') }}
    {% set select_new_alerts_query %}
        SELECT * FROM {{ ref('alerts_tests') }}
        WHERE alert_sent = FALSE and detected_at >= {{ get_alerts_time_limit(days_back) }}
    {% endset %}
    {% set select_nodes_meta_query %}
        WITH models AS (
            SELECT * FROM {{ ref('elementary', 'dbt_models') }}
        ),

        sources AS (
            SELECT * FROM {{ ref('elementary', 'dbt_sources') }}
        )

        SELECT 
            unique_id,
            meta
        FROM models
        UNION ALL 
        SELECT 
            unique_id,
            meta
        FROM sources 
    {% endset %}
    {% set nodes_meta_agate = run_query(select_nodes_meta_query) %}
    {% set nodes_meta_dicts = elementary.agate_to_dicts(nodes_meta_agate) %}
    {% set nodes_to_meta_map = {} %}
    {% for node_meta_dict in nodes_meta_dicts %}
        {% set node_id = elementary.insensitive_get_dict_value(node_meta_dict, 'unique_id') %}
        {% set node_meta = elementary.insensitive_get_dict_value(node_meta_dict, 'meta') %}
        {% do nodes_to_meta_map.update({node_id: node_meta}) %}
    {% endfor %}
    {% set alerts_agate = run_query(select_new_alerts_query) %}
    {% set test_result_alert_dicts = elementary.agate_to_dicts(alerts_agate) %}
    {% set new_alerts = [] %}
    {% for test_result_alert_dict in test_result_alert_dicts %}
        {% set test_results_query = elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_results_query') %}
        {% set test_type = elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_type') %}
        {% set status = elementary.insensitive_get_dict_value(test_result_alert_dict, 'status') | lower %}

        {% set test_rows_sample = none %}
        {%- if (test_type == 'dbt_test' and status in ['fail', 'warn']) or (test_type != 'dbt_test' and status != 'error') -%}
            {% set test_rows_sample = elementary_internal.get_test_rows_sample(test_results_query, test_type, results_sample_limit) %}
        {%- endif -%}

        

        {% set meta = fromjson(elementary.insensitive_get_dict_value(test_result_alert_dict, 'meta', '{}')) %}
        {% set model_unique_id = elementary.insensitive_get_dict_value(test_result_alert_dict, 'model_unique_id') %}
        {% set model_meta = fromjson(elementary.insensitive_get_dict_value(nodes_to_meta_map, model_unique_id, '{}')) %}
        
        {% set direct_subscribers = elementary.insensitive_get_dict_value(meta, 'subscribers', []) %}
        {% set model_subscribers = elementary.insensitive_get_dict_value(model_meta, 'subscribers', []) %}
        {% set subscribers = [] %}
        {% if direct_subscribers is string %}
            {% do subscribers.append(direct_subscribers) %}
        {% elif direct_subscribers is iterable %}}
            {% do subscribers.extend(direct_subscribers) %}
        {% endif %}
        {% if model_subscribers is string %}
            {% do subscribers.append(model_subscribers) %}
        {% elif model_subscribers is iterable %}}
            {% do subscribers.extend(model_subscribers) %}
        {% endif %}
        {% set new_alert_dict = {'id': elementary.insensitive_get_dict_value(test_result_alert_dict, 'alert_id'),
                                 'model_unique_id': model_unique_id,
                                 'test_unique_id': elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_unique_id'),
                                 'detected_at': elementary.insensitive_get_dict_value(test_result_alert_dict, 'detected_at'),
                                 'database_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'database_name'),
                                 'schema_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'schema_name'),
                                 'table_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'table_name'),
                                 'column_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'column_name'),
                                 'test_type': test_type,
                                 'test_sub_type': elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_sub_type'),
                                 'test_results_description': elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_results_description'),
                                 'owners': elementary.insensitive_get_dict_value(test_result_alert_dict, 'owners'),
                                 'tags': elementary.insensitive_get_dict_value(test_result_alert_dict, 'tags'),
                                 'test_results_query': test_results_query,
                                 'test_rows_sample': test_rows_sample,
                                 'other': elementary.insensitive_get_dict_value(test_result_alert_dict, 'other'),
                                 'test_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_name'),
                                 'test_params': elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_params'),
                                 'severity': elementary.insensitive_get_dict_value(test_result_alert_dict, 'severity'),
                                 'subscribers': subscribers,
                                 'status': status} %}
        {% do new_alerts.append(new_alert_dict) %}
    {% endfor %}
    {% do elementary.edr_log(tojson(new_alerts)) %}
{% endmacro %}

