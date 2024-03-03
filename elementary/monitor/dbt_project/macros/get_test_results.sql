{%- macro get_test_results(days_back = 7, invocations_per_test = 720, disable_passed_test_metrics = false) -%}
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
            id,
            invocation_id,
            test_execution_id,
            model_unique_id,
            test_unique_id,
            elementary_unique_id,
            detected_at,
            database_name,
            schema_name,
            table_name,
            column_name,
            test_type,
            test_sub_type,
            test_results_description,
            original_path,
            owners,
            model_owner,
            tags,
            test_tags,
            model_tags,
            meta,
            model_meta,
            test_results_query,
            other,
            test_name,
            test_params,
            severity,
            status,
            days_diff,
            invocations_rank_index,
            failures,
            result_rows
        from ordered_test_results
        where invocations_rank_index <= {{ invocations_per_test }}
        order by elementary_unique_id, invocations_rank_index desc
    {%- endset -%}

    {% set test_results_agate = elementary.run_query(select_test_results) %}
    {% set test_result_rows_agate = elementary_cli.get_result_rows_agate(days_back) %}
    {% set tests = elementary.agate_to_dicts(test_results_agate) %}
    {%- for test in tests -%}
        {% set test_rows_sample = none %}
        {% if test.invocations_rank_index == 1 %}
            {% set test_type = test.test_type %}
            {% set status = test.status | lower %}

            {% set elementary_tests_allowlist_status = ['fail', 'warn']  %}
            {% if not disable_passed_test_metrics %}
                {% do elementary_tests_allowlist_status.append('pass') %}
            {% endif %}
            {%- if (test_type == 'dbt_test' and status in ['fail', 'warn']) or (test_type != 'dbt_test' and status in elementary_tests_allowlist_status) -%}
                {% set test_rows_sample = elementary_cli.get_test_rows_sample(test.result_rows, test_result_rows_agate.get(test.id)) %}
                {# Dimension anomalies return multiple dimensions for the test rows sample, and needs to be handle differently. #}
                {# Currently we show only the anomalous for all of the dimensions. #}
                {% if test.test_sub_type == 'dimension' %}
                    {% set anomalous_rows = [] %}
                    {% set headers = [{'id': 'anomalous_value_timestamp', 'display_name': 'timestamp', 'type': 'date'}] %}
                    {% for row in test_rows_sample %}
                        {% set anomalous_row = {
                            'anomalous_value_timestamp': row['end_time'],
                            'anomalous_value_row_count': row['value'],
                            'anomalous_value_average_row_count': row['average'] | round(1)
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
                        {% do anomalous_rows.append(anomalous_row) %}
                    {% endfor %}
                    {# Adding the rest of the static headers (metrics headers) #}
                    {% do headers.extend([
                        {'id': 'anomalous_value_row_count', 'display_name': 'row count', 'type': 'int'},
                        {'id': 'anomalous_value_average_row_count', 'display_name': 'average row count', 'type': 'int'}
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
    {%- endfor -%}
    {% do return(tests) %}
{%- endmacro -%}
