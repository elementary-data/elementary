{%- macro get_test_results(days_back = 7, results_sample_limit = 5) -%}
    {% set select_test_results %}
        with elemetary_test_results as (
            select * from {{ ref('elementary', 'elementary_test_results') }}
        ),

        tests_in_last_chosen_days as (
            select *,
                  {{ dbt_utils.datediff(elementary.cast_as_timestamp('detected_at'), dbt_utils.current_timestamp(), 'day') }} as days_diff,
                  row_number() over (partition by model_unique_id, test_unique_id, column_name, test_sub_type order by detected_at desc) as row_number
                from elemetary_test_results
                where {{ dbt_utils.datediff(elementary.cast_as_timestamp('detected_at'), dbt_utils.current_timestamp(), 'day') }} < {{ days_back }}
        ),

        latest_tests_in_the_last_chosen_days as (
            select * from tests_in_last_chosen_days where row_number = 1
        )

        select id, model_unique_id, test_unique_id, detected_at, database_name, schema_name, table_name,
               column_name, test_type, test_sub_type, test_results_description, owners, tags,
               test_results_query, other, test_name, test_params, severity, status, days_diff

        from latest_tests_in_the_last_chosen_days
    {%- endset -%}
    {% set elementary_test_results_agate = run_query(select_test_results) %}
    {% set test_result_dicts_json = elementary.agate_to_json(elementary_test_results_agate) %}
    {% do elementary.edr_log(test_result_dicts_json) %}
{%- endmacro -%}

