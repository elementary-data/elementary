{% macro get_new_test_alerts(days_back, results_sample_limit=5, disable_samples=false) %}
    -- depends_on: {{ ref('alerts') }}
    {% set select_new_alerts_query %}
        with new_alerts as (
            select * from {{ ref('alerts') }}
            where alert_sent = false and {{ elementary.cast_as_timestamp('detected_at') }} >= {{ get_alerts_time_limit(days_back) }}
        ),
        models as (
            select * from {{ ref('elementary', 'dbt_models') }}
        ),
        sources as (
            select * from {{ ref('elementary', 'dbt_sources') }}
        ),
        tests as (
            select * from {{ ref('elementary', 'dbt_tests') }}
        ),
        artifacts_meta as (
            select unique_id, meta from models
            union all
            select unique_id, meta from sources
        )

        select new_alerts.*, tests.meta as test_meta, artifacts_meta.meta as model_meta
        from new_alerts
        left join tests on new_alerts.test_unique_id = tests.unique_id
        left join artifacts_meta on new_alerts.model_unique_id = artifacts_meta.unique_id
    {% endset %}

    {% set alerts_agate = run_query(select_new_alerts_query) %}
    {% set test_result_alert_dicts = elementary.agate_to_dicts(alerts_agate) %}
    {% set new_alerts = [] %}
    {% for test_result_alert_dict in test_result_alert_dicts %}
        {% set test_results_query = elementary.insensitive_get_dict_value(test_result_alert_dict, 'alert_results_query') %}
        {% set test_type = elementary.insensitive_get_dict_value(test_result_alert_dict, 'alert_type') %}
        {% set status = elementary.insensitive_get_dict_value(test_result_alert_dict, 'status') | lower %}

        {% set test_rows_sample = none %}
        {%- if not disable_samples and ((test_type == 'dbt_test' and status in ['fail', 'warn']) or (test_type != 'dbt_test' and status != 'error')) -%}
            {% set test_rows_sample = elementary_internal.get_test_rows_sample(test_result_alert_dict, test_results_query, test_type, results_sample_limit) %}
        {%- endif -%}

        {% set test_meta = elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_meta') %}
        {% set model_unique_id = elementary.insensitive_get_dict_value(test_result_alert_dict, 'model_unique_id') %}
        {% set model_meta = elementary.insensitive_get_dict_value(test_result_alert_dict, 'model_meta') %}

        {% set new_alert_dict = {'id': elementary.insensitive_get_dict_value(test_result_alert_dict, 'alert_id'),
                                 'model_unique_id': model_unique_id,
                                 'test_unique_id': elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_unique_id'),
                                 'detected_at': elementary.insensitive_get_dict_value(test_result_alert_dict, 'detected_at'),
                                 'database_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'database_name'),
                                 'schema_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'schema_name'),
                                 'table_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'table_name'),
                                 'column_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'column_name'),
                                 'test_type': test_type,
                                 'test_sub_type': elementary.insensitive_get_dict_value(test_result_alert_dict, 'sub_type'),
                                 'test_results_description': elementary.insensitive_get_dict_value(test_result_alert_dict, 'alert_description'),
                                 'owners': elementary.insensitive_get_dict_value(test_result_alert_dict, 'owners'),
                                 'tags': elementary.insensitive_get_dict_value(test_result_alert_dict, 'tags'),
                                 'test_results_query': test_results_query,
                                 'test_rows_sample': test_rows_sample,
                                 'other': elementary.insensitive_get_dict_value(test_result_alert_dict, 'other'),
                                 'test_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_name'),
                                 'test_short_name': elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_short_name'),
                                 'test_params': elementary.insensitive_get_dict_value(test_result_alert_dict, 'test_params'),
                                 'severity': elementary.insensitive_get_dict_value(test_result_alert_dict, 'severity'),
                                 'test_meta': test_meta,
                                 'model_meta': model_meta,
                                 'status': status} %}
        {% do new_alerts.append(new_alert_dict) %}
    {% endfor %}
    {% do elementary.edr_log(tojson(new_alerts)) %}
{% endmacro %}

