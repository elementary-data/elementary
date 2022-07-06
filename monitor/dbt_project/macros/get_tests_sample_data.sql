{% macro get_tests_sample_data(days_back = 7, metrics_sample_limit = 5) %}
    {% set select_test_results %}
        with elemetary_test_results as (
            select * from {{ ref('elementary', 'elementary_test_results') }}
        ),

        tests_in_last_chosen_days as (
            select *,
                  row_number() over (partition by model_unique_id, test_unique_id, column_name, test_sub_type order by detected_at desc) as row_number
            from elemetary_test_results
            where {{ dbt_utils.datediff(elementary.cast_as_timestamp('detected_at'), dbt_utils.current_timestamp(), 'day') }} < {{ days_back }}
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
            status
        from latest_tests_in_the_last_chosen_days
    {%- endset -%}
    {% set elementary_tests_results_agate = run_query(select_test_results) %}
    {% set tests = elementary.agate_to_dicts(elementary_tests_results_agate) %}
    {% set tests_metrics = {} %}
    {%- for test in tests -%}
        {% set test_unique_id = elementary.insensitive_get_dict_value(test, 'test_unique_id') %}
        {% set test_results_query = elementary.insensitive_get_dict_value(test, 'test_results_query') %}
        {% set test_type = elementary.insensitive_get_dict_value(test, 'test_type') %}
        {% set status = elementary.insensitive_get_dict_value(test, 'status') | lower %}

        {% set test_rows_sample = none %}
        {%- if (test_type == 'dbt_test' and status in ['fail', 'warn']) or (test_type != 'dbt_test' and status != 'error') -%}
            {% set test_rows_sample = elementary_internal.get_test_rows_sample(test_results_query, test_type, metrics_sample_limit) %}
        {%- endif -%}
        {% set sub_test_unique_id = get_sub_test_unique_id(
            model_unique_id=elementary.insensitive_get_dict_value(test, 'model_unique_id'),
            test_unique_id=elementary.insensitive_get_dict_value(test, 'test_unique_id'),
            test_sub_type=elementary.insensitive_get_dict_value(test, 'test_sub_type'),
            column_name=elementary.insensitive_get_dict_value(test, 'column_name'),
        ) %}
        {% do tests_metrics.update({sub_test_unique_id: test_rows_sample}) %}
    {%- endfor -%}
    {% do elementary.edr_log(tojson(tests_metrics)) %}
{% endmacro %}
