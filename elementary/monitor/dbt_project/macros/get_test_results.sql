{%- macro get_test_results(days_back = 7, results_sample_limit = 5) -%}
    {% set select_test_results %}
        with test_results as (
            {{ elementary_internal.current_tests_run_results_query(days_back=days_back) }}
        ),

        tests_in_last_chosen_days as (
            select *,
                  {{ elementary.datediff(elementary.cast_as_timestamp('detected_at'), elementary.current_timestamp(), 'day') }} as days_diff,
                  row_number() over (partition by model_unique_id, test_unique_id, column_name, test_sub_type order by detected_at desc) as row_number
                from test_results
        ),

        latest_tests_in_the_last_chosen_days as (
            select * from tests_in_last_chosen_days where row_number = 1
        )

        select 
            id,
            model_unique_id,
            test_unique_id,
            detected_at,
            database_name,
            schema_name,
            table_name,
            column_name,
            test_type,
            test_sub_type,
            test_results_description,
            owners,
            tags,
            meta,
            test_results_query,
            other,
            test_name,
            test_params,
            severity,
            status,
            days_diff
        from latest_tests_in_the_last_chosen_days
    {%- endset -%}
    {% set test_results_agate = run_query(select_test_results) %}
    {% set test_result_dicts_json = elementary.agate_to_json(test_results_agate) %}
    {% do elementary.edr_log(test_result_dicts_json) %}
{%- endmacro -%}

