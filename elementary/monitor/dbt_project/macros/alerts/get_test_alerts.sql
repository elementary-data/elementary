{% macro get_pending_test_alerts(days_back, disable_samples=false) %}
    -- depends_on: {{ ref('alerts') }}
    {% set select_pending_alerts_query %}
        with alerts_in_time_limit as (
            select * from {{ ref('alerts') }}
            where {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit(days_back) }}
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
        ),

        extended_alerts as (
            select 
                alerts_in_time_limit.alert_id,
                {# Generate elementary unique id which is used to identify between tests, and set it as alert_class_id #}
                coalesce(alerts_in_time_limit.test_unique_id, 'None') || '.' || coalesce(alerts_in_time_limit.column_name, 'None') || '.' || coalesce(alerts_in_time_limit.sub_type, 'None') as alert_class_id,
                case
                    when alerts_in_time_limit.alert_type = 'schema_change' then alerts_in_time_limit.test_unique_id
                    {# In old versions of elementary, elementary_test_results doesn't contain test_short_name, so we use dbt_test short_name. #}
                    when tests.short_name = 'dimension_anomalies' then alerts_in_time_limit.test_unique_id
                    else coalesce(alerts_in_time_limit.test_unique_id, 'None') || '.' || coalesce(alerts_in_time_limit.column_name, 'None') || '.' || coalesce(alerts_in_time_limit.sub_type, 'None')
                end as elementary_unique_id,
                alerts_in_time_limit.data_issue_id,
                alerts_in_time_limit.test_execution_id,
                alerts_in_time_limit.test_unique_id,
                alerts_in_time_limit.model_unique_id,
                alerts_in_time_limit.detected_at,
                alerts_in_time_limit.database_name,
                alerts_in_time_limit.schema_name,
                alerts_in_time_limit.table_name,
                alerts_in_time_limit.column_name,
                alerts_in_time_limit.alert_type,
                alerts_in_time_limit.sub_type,
                alerts_in_time_limit.alert_description,
                alerts_in_time_limit.owners,
                alerts_in_time_limit.tags,
                alerts_in_time_limit.alert_results_query,
                alerts_in_time_limit.other,
                alerts_in_time_limit.test_name,
                alerts_in_time_limit.test_params,
                alerts_in_time_limit.severity,
                alerts_in_time_limit.status,
                alerts_in_time_limit.result_rows,
                alerts_in_time_limit.test_short_name,
                {# backwards compatibility #}
                case
                    when alerts_in_time_limit.suppression_status is NULL and alerts_in_time_limit.alert_sent = TRUE then 'sent'
                    when alerts_in_time_limit.suppression_status is NULL and alerts_in_time_limit.alert_sent = FALSE then 'pending'
                    else suppression_status
                end as suppression_status,
                alerts_in_time_limit.sent_at,
                tests.meta as test_meta,
                artifacts_meta.meta as model_meta
            from alerts_in_time_limit
            left join tests on alerts_in_time_limit.test_unique_id = tests.unique_id
            left join artifacts_meta on alerts_in_time_limit.model_unique_id = artifacts_meta.unique_id
        )

        select *
        from extended_alerts
        where suppression_status = 'pending'
    {% endset %}

    {% set alerts_agate = elementary.run_query(select_pending_alerts_query) %}
    {% set test_result_rows_agate = elementary_cli.get_result_rows_agate(days_back) %}
    {% set test_result_alert_dicts = elementary.agate_to_dicts(alerts_agate) %}
    {% set pending_alerts = [] %}
    {% for alert in test_result_alert_dicts %}
        {% set test_type = alert.alert_type %}
        {% set status = alert.status | lower %}

        {% set test_rows_sample = none %}
        {%- if not disable_samples and ((test_type == 'dbt_test' and status in ['fail', 'warn']) or (test_type != 'dbt_test' and status != 'error')) -%}
            {% set test_rows_sample = elementary_cli.get_test_rows_sample(alert.result_rows, test_result_rows_agate.get(alert.alert_id)) %}
        {%- endif -%}
        {% set pending_alert_dict = {'id': alert.alert_id,
                                 'alert_class_id': alert.alert_class_id,
                                 'model_unique_id': alert.model_unique_id,
                                 'test_unique_id': alert.test_unique_id,
                                 'detected_at': alert.detected_at,
                                 'database_name': alert.database_name,
                                 'schema_name': alert.schema_name,
                                 'table_name': alert.table_name,
                                 'column_name': alert.column_name,
                                 'test_type': test_type,
                                 'test_sub_type': alert.sub_type,
                                 'test_results_description': alert.alert_description,
                                 'owners': alert.owners,
                                 'tags': alert.tags,
                                 'test_results_query': alert.alert_results_query,
                                 'test_rows_sample': test_rows_sample,
                                 'other': alert.other,
                                 'test_name': alert.test_name,
                                 'test_short_name': alert.test_short_name,
                                 'test_params': alert.test_params,
                                 'severity': alert.severity,
                                 'test_meta': alert.test_meta,
                                 'model_meta': alert.model_meta,
                                 'suppression_status': alert.suppression_status,
                                 'sent_at': alert.sent_at,
                                 'status': status,
                                 'elementary_unique_id': alert.elementary_unique_id} %}
        {% do pending_alerts.append(pending_alert_dict) %}
    {% endfor %}
    {% do return(pending_alerts) %}
{% endmacro %}


{% macro get_last_test_alert_sent_times(days_back) %}
    -- depends_on: {{ ref('alerts') }}
    {% set select_last_alert_sent_times_query %}
        with alerts_in_time_limit as (
            select
                {# Generate elementary unique id which is used to identify between tests, and set it as alert_class_id #}
                coalesce(test_unique_id, 'None') || '.' || coalesce(column_name, 'None') || '.' || coalesce(sub_type, 'None') as alert_class_id,
                case
                    when suppression_status is NULL and alert_sent = TRUE then 'sent'
                    when suppression_status is NULL and alert_sent = FALSE then 'pending'
                    else suppression_status
                end as suppression_status,
                sent_at
            from {{ ref('alerts') }}
            where {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit(days_back) }}
        )

        select 
            alert_class_id,
            max(sent_at) as last_sent_at
        from alerts_in_time_limit
        where suppression_status = 'sent'
        group by alert_class_id
    {% endset %}

    {% set alerts_agate = run_query(select_last_alert_sent_times_query) %}
    {% set last_alert_sent_time_result_dicts = elementary.agate_to_dicts(alerts_agate) %}
    {% set last_alert_times = {} %}
    {% for last_alert_sent_time_result_dict in last_alert_sent_time_result_dicts %}
        {% do last_alert_times.update({
            last_alert_sent_time_result_dict.get('alert_class_id'): last_alert_sent_time_result_dict.get('last_sent_at')
        }) %}
    {% endfor %}
    {% do return(last_alert_times) %}
{% endmacro %}
