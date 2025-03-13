{% macro populate_test_alerts(days_back=1, disable_samples=false) %}
    {% set test_result_rows_agate = not disable_samples and elementary_cli.get_result_rows_agate(days_back) %}

    {% set test_alerts = [] %}
    {% set raw_test_alerts_agate = run_query(elementary_cli.populate_test_alerts_query(days_back)) %}
    {% set raw_test_alerts = elementary.agate_to_dicts(raw_test_alerts_agate) %}
    {% for raw_test_alert in raw_test_alerts %}
        {% set test_type = raw_test_alert.alert_type %}
        {% set status = raw_test_alert.status | lower %}

        {% set test_rows_sample = none %}
        {%- if not disable_samples and ((test_type == 'dbt_test' and status in ['fail', 'warn']) or (test_type != 'dbt_test' and status != 'error')) -%}
            {% set test_rows_sample = elementary_cli.get_test_rows_sample(raw_test_alert.result_rows, test_result_rows_agate.get(raw_test_alert.alert_id)) %}
        {%- endif -%}

        {% set test_alert_data = {
            'id': raw_test_alert.alert_id,
            'alert_class_id': raw_test_alert.alert_class_id,
            'model_unique_id': raw_test_alert.model_unique_id,
            'test_unique_id': raw_test_alert.test_unique_id,
            'detected_at': raw_test_alert.detected_at,
            'database_name': raw_test_alert.database_name,
            'schema_name': raw_test_alert.schema_name,
            'table_name': raw_test_alert.table_name,
            'column_name': raw_test_alert.column_name,
            'test_type': test_type,
            'test_sub_type': raw_test_alert.sub_type,
            'test_description': raw_test_alert.test_description,
            'test_results_description': raw_test_alert.alert_description,
            'owners': raw_test_alert.owners,
            'tags': raw_test_alert.tags,
            'test_results_query': raw_test_alert.alert_results_query,
            'test_rows_sample': test_rows_sample,
            'other': raw_test_alert.other,
            'test_name': raw_test_alert.test_name,
            'test_short_name': raw_test_alert.test_short_name,
            'test_params': raw_test_alert.test_params,
            'severity': raw_test_alert.severity,
            'test_meta': raw_test_alert.test_meta,
            'model_meta': raw_test_alert.model_meta,
            'status': status,
            'elementary_unique_id': raw_test_alert.elementary_unique_id
        } 
        %}

        {% set test_alert = elementary_cli.generate_alert_object(
            raw_test_alert.alert_id,
            raw_test_alert.alert_class_id,
            'test',
            raw_test_alert.detected_at,
            raw_test_alert.created_at,
            test_alert_data,
        ) %}
        {% do test_alerts.append(test_alert) %}
    {% endfor %}
    {% do return(test_alerts) %}
{% endmacro %}


{% macro populate_test_alerts_query(days_back=1) %}
    with elementary_test_results as (
        select * from {{ ref('elementary_test_results') }}
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

    failed_tests as (
        select
            id as alert_id,
            data_issue_id,
            test_execution_id,
            test_unique_id,
            model_unique_id,
            detected_at,
            database_name,
            schema_name,
            table_name,
            column_name,
            test_type,
            test_sub_type as sub_type,
            test_results_description,
            owners,
            tags,
            test_results_query,
            other,
            test_name,
            test_short_name,
            test_params,
            severity,
            status,
            result_rows
        from elementary_test_results
        where lower(status) != 'pass'
        and {{ elementary.edr_cast_as_timestamp('detected_at') }} > {{ elementary.edr_timeadd('day', -1 * days_back, elementary.edr_current_timestamp()) }}
    )

    select distinct
        failed_tests.alert_id,
        {# Generate elementary unique id which is used to identify between tests, and set it as alert_class_id #}
        coalesce(failed_tests.test_unique_id, 'None') || '.' || coalesce(failed_tests.column_name, 'None') || '.' || coalesce(failed_tests.sub_type, 'None') as alert_class_id,
        case
            when failed_tests.test_type = 'schema_change' then failed_tests.test_unique_id
            {# In old versions of elementary, elementary_test_results doesn't contain test_short_name, so we use dbt_test short_name. #}
            when tests.short_name = 'dimension_anomalies' then failed_tests.test_unique_id
            else coalesce(failed_tests.test_unique_id, 'None') || '.' || coalesce(failed_tests.column_name, 'None') || '.' || coalesce(failed_tests.sub_type, 'None')
        end as elementary_unique_id,
        failed_tests.data_issue_id,
        failed_tests.test_execution_id,
        failed_tests.test_unique_id,
        failed_tests.model_unique_id,
        failed_tests.database_name,
        failed_tests.detected_at,
        {{ elementary.edr_current_timestamp() }} as created_at,
        failed_tests.schema_name,
        failed_tests.table_name,
        failed_tests.column_name,
        failed_tests.test_type as alert_type,
        failed_tests.sub_type,
        failed_tests.test_results_description as alert_description,
        failed_tests.owners,
        failed_tests.tags,
        failed_tests.test_results_query as alert_results_query,
        failed_tests.other,
        failed_tests.test_name,
        failed_tests.test_short_name,
        failed_tests.test_params,
        failed_tests.severity,
        failed_tests.status,
        failed_tests.result_rows,
        tests.meta as test_meta,
        tests.description as test_description,
        artifacts_meta.meta as model_meta
    from failed_tests
    left join tests on failed_tests.test_unique_id = tests.unique_id
    left join artifacts_meta on failed_tests.model_unique_id = artifacts_meta.unique_id
    where failed_tests.alert_id not in (
        {# "this" is referring to "alerts_v2" - we are executing it using a post_hook over "alerts_v2" #}
        select alert_id from {{ this }}
    )
{% endmacro %}
