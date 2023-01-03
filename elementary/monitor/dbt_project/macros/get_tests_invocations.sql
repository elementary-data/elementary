{%- macro get_tests_invocations(invocations_per_test = 720, days_back = 7) -%}
    {% set tests_invocations_query %}
        with test_results as (
            {{ elementary_internal.current_tests_run_results_query(days_back=days_back) }}
        ),

        test_results_in_last_chosen_days as (
            select *,
                row_number() over (partition by test_sub_type_unique_id order by detected_at desc) as row_number
            from test_results
        )

        select
            invocation_id,
            model_unique_id, 
            test_unique_id,
            test_sub_type_unique_id,
            test_sub_type,
            test_type,
            column_name,
            detected_at,
            test_results_description,
            test_execution_id,
            status
        from test_results_in_last_chosen_days
        order by detected_at
    {% endset %}
    {% set tests_invocations_agate = run_query(tests_invocations_query) %}
    {% set tests_invocations_results = elementary.agate_to_json(tests_invocations_agate) %}
    {% do elementary.edr_log(tests_invocations_results) %}
{%- endmacro -%}