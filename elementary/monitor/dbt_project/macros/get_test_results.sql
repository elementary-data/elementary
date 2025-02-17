{%- macro get_test_results(days_back = 7, invocations_per_test = 720, disable_passed_test_metrics = false) -%}
    {% set elementary_tests_allowlist_status = ['fail', 'warn'] if disable_passed_test_metrics else ['fail', 'warn', 'pass']  %}
    
    {% set select_test_results %}
        with test_results as (
            {{ elementary_cli.current_tests_run_results_query(days_back=days_back) }}
        ),

        ordered_test_results as (
            select
                *,
                {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('detected_at'), elementary.edr_current_timestamp(), 'day') }} as days_diff,
                {# When we split test into multiple test results, we want to have the same invocation order for the test results from the same run so we use rank. #}
                rank() over (partition by elementary_unique_id order by detected_at desc) as invocations_rank_index
            from test_results
        )

        select 
            test_results.id,
            test_results.invocation_id,
            test_results.test_execution_id,
            test_results.model_unique_id,
            test_results.test_unique_id,
            test_results.elementary_unique_id,
            test_results.detected_at,
            test_results.database_name,
            test_results.schema_name,
            test_results.table_name,
            test_results.column_name,
            test_results.test_type,
            test_results.test_sub_type,
            test_results.test_results_description,
            test_results.original_path,
            test_results.package_name,
            test_results.owners,
            test_results.model_owner,
            test_results.tags,
            test_results.test_tags,
            test_results.model_tags,
            test_results.meta,
            test_results.model_meta,
            case when test_results.invocations_rank_index = 1 then test_results.test_results_query else NULL end as test_results_query,
            test_results.other,
            test_results.test_name,
            test_results.test_params,
            test_results.severity,
            test_results.status,
            test_results.execution_time,
            test_results.days_diff,
            test_results.invocations_rank_index,
            test_results.failures,
            test_results.result_rows
        from ordered_test_results as test_results
        where test_results.invocations_rank_index <= {{ invocations_per_test }}
        order by test_results.elementary_unique_id, test_results.invocations_rank_index desc
    {%- endset -%}

    {% set test_results = [] %}

    {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
    {% set ordered_test_results_relation = elementary_cli.create_temp_table(elementary_database, elementary_schema, 'ordered_test_results', select_test_results) %}

    {% set test_results_agate_sql %}
        select * from {{ ordered_test_results_relation }}
    {% endset %}

    {% set valid_ids_query %}
        select distinct id 
        from {{ ordered_test_results_relation }}
        where invocations_rank_index = 1
    {% endset %}

    {% set test_results_agate = elementary.run_query(test_results_agate_sql) %}
    {% set test_result_rows_agate = elementary_cli.get_result_rows_agate(days_back, valid_ids_query) %}
    {% set tests = elementary.agate_to_dicts(test_results_agate) %}

    {% set filtered_tests = [] %}
    {% for test in tests %}
        {% set test_meta = fromjson(test.meta) %}
        {% if test_meta.get("elementary", {}).get("include", true) %}
            {% do filtered_tests.append(test) %}
        {% endif %}
    {% endfor %}

    {% for test in filtered_tests %}
        {% set test_rows_sample = none %}
        {% if test.invocations_rank_index == 1 %}
            {% set test_type = test.test_type %}
            {% set test_params = fromjson(test.test_params) %}
            {% set status = test.status | lower %}

            {%- if (test_type == 'dbt_test' and status in ['fail', 'warn']) or (test_type != 'dbt_test' and status in elementary_tests_allowlist_status) -%}
                {% set test_rows_sample = elementary_cli.get_test_rows_sample(test.result_rows, test_result_rows_agate.get(test.id)) %}
                {# Dimension anomalies return multiple dimensions for the test rows sample, and needs to be handle differently. #}
                {# Currently we show only the anomalous for all of the dimensions. #}
                {% if test.test_sub_type == 'dimension' or test_params.dimensions %}
                    {% if test.test_sub_type == 'dimension' %}
                      {% set metric_name = 'row_count' %}
                    {% elif test_params.dimensions %}
                      {% set metric_name = test.test_sub_type %}
                    {% endif %}
                    {% set anomalous_rows = [] %}
                    {% set headers = [{'id': 'anomalous_value_timestamp', 'display_name': 'timestamp', 'type': 'date'}] %}
                    {% for row in test_rows_sample %}
                        {% set anomalous_row = {
                        'anomalous_value_timestamp': row['end_time'],
                        'anomalous_value_' ~ metric_name: row['value'],
                        'anomalous_value_average_' ~ metric_name: row['average'] | round(1)
                        } %}
                        {% set dimensions = row['dimension'].split('; ') %}
                        {% set diemsions_values = row['dimension_value'].split('; ') %}
                        {% for index in range(dimensions | length) %}
                            {% do anomalous_row.update({dimensions[index]: diemsions_values[index]}) %}
                        {% endfor %}
                        {% if loop.last %}
                            {# Adding dimensions to the headers #}
                            {% for index in range(dimensions | length) %}
                                {% do headers.append({'id': dimensions[index], 'display_name': dimensions[index], 'type': 'str'},) %}
                            {% endfor %}
                        {% endif %}
                        {% if row['is_anomalous'] %}
                          {% do anomalous_rows.append(anomalous_row) %}
                        {% endif %}
                    {% endfor %}
                    {# Adding the rest of the static headers (metrics headers) #}
                    {% do headers.extend([
                        {'id': 'anomalous_value_' ~ metric_name, 'display_name': ' '.join(metric_name.split('_')), 'type': 'int'},
                        {'id': 'anomalous_value_average_' ~ metric_name, 'display_name': 'average ' ~ ' '.join(metric_name.split('_')), 'type': 'int'}
                    ]) %}
                    {% set test_rows_sample = {
                        'headers': headers,
                        'test_rows_sample': anomalous_rows
                    } %}
                {% endif %}
            {%- endif -%}
        {% endif %}
        {# Adding sample data to test results #}
        {% do test.update({"sample_data": test_rows_sample}) %}
        {% do test_results.append(test) %}
    {%- endfor -%}

    {% do return(test_results) %}
{%- endmacro -%}
