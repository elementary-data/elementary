{%- macro get_test_results(days_back = 7, results_sample_limit = 5, invocation_id = none) -%}
    {% set select_test_results %}
        with latest_tests_in_the_last_chosen_days as (
            {{ elementary_internal.latest_tests_in_the_last_chosen_days(days_back=days_back, invocation_id=invocation_id) }}
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

