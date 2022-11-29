{% macro get_tests_sample_data(days_back=7, metrics_sample_limit=5, disable_passed_test_metrics=false) %}
    {% set select_test_results %}
        with test_results as (
            {{ elementary_internal.current_tests_run_results_query(days_back=days_back) }}
        ),

        tests_in_last_chosen_days as (
            select *,
                  row_number() over (partition by model_unique_id, test_unique_id, column_name, test_sub_type order by detected_at desc) as row_number
            from test_results
        ),

        latest_tests_in_the_last_chosen_days as (
            select * from tests_in_last_chosen_days where row_number = 1
        )

        select 
            model_unique_id,
            test_unique_id,
            test_type,
            test_sub_type,
            column_name,
            test_results_query,
            status,
            result_rows
        from latest_tests_in_the_last_chosen_days
    {%- endset -%}
    {% set tests_results_agate = run_query(select_test_results) %}
    {% set tests = elementary.agate_to_dicts(tests_results_agate) %}
    {% set tests_metrics = {} %}
    {%- for test in tests -%}
        {% set test_unique_id = elementary.insensitive_get_dict_value(test, 'test_unique_id') %}
        {% set test_results_query = elementary.insensitive_get_dict_value(test, 'test_results_query') %}
        {% set test_type = elementary.insensitive_get_dict_value(test, 'test_type') %}
        {% set test_sub_type = elementary.insensitive_get_dict_value(test, 'test_sub_type') %}
        {% set status = elementary.insensitive_get_dict_value(test, 'status') | lower %}

        {% set elementary_tests_allowlist_status = ['fail', 'warn']  %}
        {% if not disable_passed_test_metrics %}
            {% do elementary_tests_allowlist_status.append('pass') %}
        {% endif %}
        {% set test_rows_sample = elementary_internal.get_test_rows_sample(test, test_results_query, test_type, metrics_sample_limit) %}
        {%- if (test_type == 'dbt_test' and status in ['fail', 'warn']) or (test_type != 'dbt_test' and status in elementary_tests_allowlist_status) -%}
            {# Dimension anomalies return multiple dimensions for the test rows sample, and needs to be handle differently. #}
            {# Currently we show only the anomalous for all of the dimensions. #}
            {% if test_sub_type == 'dimension' %}
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
        {% set sub_test_unique_id = get_sub_test_unique_id(
            model_unique_id=elementary.insensitive_get_dict_value(test, 'model_unique_id'),
            test_unique_id=elementary.insensitive_get_dict_value(test, 'test_unique_id'),
            test_sub_type=test_sub_type,
            column_name=elementary.insensitive_get_dict_value(test, 'column_name'),
        ) %}
        {% do tests_metrics.update({sub_test_unique_id: test_rows_sample}) %}
    {%- endfor -%}
    {% do elementary.edr_log(tojson(tests_metrics)) %}
{% endmacro %}
