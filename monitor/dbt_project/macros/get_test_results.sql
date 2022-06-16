{%- macro get_test_results(results_sample_limit = 5) -%}
    {% set select_test_results %}
        with elemetary_test_results as (
            select * from {{ ref('elementary', 'elementary_test_results') }}
        ),

        tests_in_last_30_days as (
            select *,
                  {{ dbt_utils.datediff(elementary.cast_as_timestamp('detected_at'), dbt_utils.current_timestamp(), 'day') }} as days_diff,
                  row_number() over (partition by model_unique_id, test_unique_id, column_name, test_sub_type order by detected_at desc) as row_number
                from elemetary_test_results
                where {{ dbt_utils.datediff(elementary.cast_as_timestamp('detected_at'), dbt_utils.current_timestamp(), 'day') }} < 30
        ),

        latest_tests_in_the_last_30_days as (
            select * from tests_in_last_30_days where row_number = 1
        )

        select id, model_unique_id, test_unique_id, detected_at, database_name, schema_name, table_name,
               column_name, test_type, test_sub_type, test_results_description, owners, tags,
               test_results_query, other, test_name, test_params, severity, status, days_diff

        from latest_tests_in_the_last_30_days
    {%- endset -%}
    {% set elementary_test_results_agate = run_query(select_test_results) %}
    {% set test_result_dicts = elementary.agate_to_dicts(elementary_test_results_agate) %}
    {% set test_result_with_sample_dicts = [] %}
    {%- for test_result_dict in test_result_dicts -%}
        {% set test_results_query = elementary.insensitive_get_dict_value(test_result_dict, 'test_results_query') %}
        {% set test_type = elementary.insensitive_get_dict_value(test_result_dict, 'test_type') %}
        {% set status = elementary.insensitive_get_dict_value(test_result_dict, 'status') | lower %}

        {% set test_rows_sample = none %}
        {%- if status != 'error'-%}
            {% set test_rows_sample = elementary_internal.get_test_rows_sample(test_results_query, test_type, results_sample_limit) %}
        {%- endif -%}
        {% do test_result_dict.update({'test_rows_sample': test_rows_sample}) %}
        {% do test_result_with_sample_dicts.append(test_result_dict) %}
    {%- endfor -%}
    {% do elementary.edr_log(tojson(test_result_with_sample_dicts)) %}
{%- endmacro -%}

