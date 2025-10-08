{% macro validate_no_timestamp_anomalies() %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}

    {# Validating row count for no timestamp table anomaly #}
    {% set no_timestamp_row_count_validation_query %}
        select distinct table_name
        from {{ alerts_relation }}
        where status in ('fail', 'warn') and sub_type = 'row_count'
        and upper(table_name) = 'NO_TIMESTAMP_ANOMALIES'
    {% endset %}
    {% set results = elementary.result_column_to_list(no_timestamp_row_count_validation_query) %}
    {{ assert_lists_contain_same_items(results, ['no_timestamp_anomalies']) }}

    {# Validating any column anomaly with no timestamp #}
    {% set no_timestamp_column_validation_alerts %}
        select column_name, sub_type
        from {{ alerts_relation }}
        where status in ('fail', 'warn') and upper(table_name) = 'NO_TIMESTAMP_ANOMALIES'
          and column_name is not NULL
        group by 1,2
    {% endset %}
    {% set alert_rows = run_query(no_timestamp_column_validation_alerts) %}
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
    {{ assert_lists_contain_same_items(results, ['null_count_str']) }}
{% endmacro %}

{% macro validate_error_test() %}
    {% set alerts_relation = ref('test_alerts_union') %}

    {# Validating alert for error test was created #}
    {% set error_test_validation_query %}
        with error_tests as (
            select
                distinct test_name,
                {{ elementary.contains('tags', 'error_test') }} as error_tag
            from {{ alerts_relation }}
            where status = 'error'
        )
        select
            case when error_tag = true then 'error'
            else 'error: ' || test_name
            end as error_tests
        from error_tests
    {% endset %}
    {% set results = elementary.result_column_to_list(error_test_validation_query) | unique | list %}
    {{ assert_lists_contain_same_items(results, ['error']) }}
{% endmacro %}

{% macro validate_error_model() %}
    {% set alerts_relation = ref('alerts_dbt_models') %}

    {% set error_model_validation_query %}
        select distinct status
        from {{ alerts_relation }}
        where status = 'error' and materialization != 'snapshot'
    {% endset %}
    {% set results = elementary.result_column_to_list(error_model_validation_query) %}
    {{ assert_lists_contain_same_items(results, ['error']) }}
{% endmacro %}

{% macro validate_error_snapshot() %}
    {% set alerts_relation = ref('alerts_dbt_models') %}

    {% set error_snapshot_validation_query %}
        select distinct status
        from {{ alerts_relation }}
        where status = 'error' and materialization = 'snapshot'
    {% endset %}
    {% set results = elementary.result_column_to_list(error_snapshot_validation_query) %}
    {{ assert_lists_contain_same_items(results, ['error']) }}
{% endmacro %}

{% macro validate_regular_tests() %}
    {% set alerts_relation = ref('alerts_dbt_tests') %}
    {% set dbt_test_alerts %}
        select table_name, column_name, test_name
        from {{ alerts_relation }}
        where status in ('fail', 'warn')
        group by 1, 2, 3
    {% endset %}
    {% set alert_rows = run_query(dbt_test_alerts) %}
    {% set found_tables = [] %}
    {% set found_columns = [] %}
    {% set found_tests = [] %}
    {% for row in alert_rows %}
        {%- if row[0] -%}
            {% do found_tables.append(row[0]) %}
        {%- endif -%}
        {%- if row[1] -%}
            {% do found_columns.append(row[1]) %}
        {%- endif -%}
        {%- if row[2] -%}
            {% do found_tests.append(row[2]) %}
        {%- endif -%}
    {% endfor %}
    {{ assert_list1_in_list2(['string_column_anomalies', 'numeric_column_anomalies', 'any_type_column_anomalies', 'any_type_column_anomalies_validation', 'numeric_column_anomalies_training'], found_tables) }}
    {{ assert_list1_in_list2(['min_length', 'null_count_int'], found_columns) }}
    {{ assert_list1_in_list2(['relationships', 'singular_test_with_no_ref', 'singular_test_with_one_ref', 'singular_test_with_two_refs', 'singular_test_with_source_ref', 'generic_test_on_model', 'generic_test_on_column'], found_tests) }}

{% endmacro %}

{% macro validate_dbt_artifacts() %}
    {% set dbt_models_relation = ref('dbt_models') %}
    {% set dbt_models_query %}
        select distinct name from {{ dbt_models_relation }}
    {% endset %}
    {% set models = elementary.result_column_to_list(dbt_models_query) %}
    {{ assert_value_in_list('any_type_column_anomalies', models, context='dbt_models') }}
    {{ assert_value_in_list('numeric_column_anomalies', models, context='dbt_models') }}
    {{ assert_value_in_list('string_column_anomalies', models, context='dbt_models') }}

    {% set dbt_sources_relation = ref('dbt_sources') %}
    {% set dbt_sources_query %}
        select distinct name from {{ dbt_sources_relation }}
    {% endset %}
    {% set sources = elementary.result_column_to_list(dbt_sources_query) %}
    {{ assert_value_in_list('any_type_column_anomalies_training', sources, context='dbt_sources') }}
    {{ assert_value_in_list('string_column_anomalies_training', sources, context='dbt_sources') }}
    {{ assert_value_in_list('any_type_column_anomalies_validation', sources, context='dbt_sources') }}

    {% set dbt_tests_relation = ref('dbt_tests') %}
    {% set dbt_tests_query %}
        select distinct name from {{ dbt_tests_relation }}
    {% endset %}
    {% set tests = elementary.result_column_to_list(dbt_tests_query) %}

    {% set dbt_run_results = ref('dbt_run_results') %}
    {% set dbt_run_results_query %}
        select distinct name from {{ dbt_run_results }} where resource_type in ('model', 'test')
    {% endset %}
    {% set run_results = elementary.result_column_to_list(dbt_run_results_query) %}
    {% set all_executable_nodes = [] %}
    {% do all_executable_nodes.extend(models) %}
    {% do all_executable_nodes.extend(tests) %}
    {{ assert_list1_in_list2(run_results, all_executable_nodes, context='dbt_run_results') }}


    {% set query %}
    select distinct invocations.invocation_id, results.invocation_id from {{ ref("dbt_invocations") }} invocations
    full outer join {{ ref("dbt_run_results") }} results
    on invocations.invocation_id = results.invocation_id
    where invocations.invocation_id is null or results.invocation_id is null
    {% endset %}
    {% set result = elementary.run_query(query) %}
    {% do assert_empty_table(result, "dbt_invocations") %}
{% endmacro %}