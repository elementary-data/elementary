{# 
    In the future we will want to merge between "get_test_results" and "get_tests_sample_data"
    So we currently have a small duplication in "latest_tests_in_the_last_chosen_days" for simplicity 
#}
{%- macro get_test_results_v2(days_back = 7, results_sample_limit = 5, invocation_id = none) -%}
    {% set select_test_results %}
        with test_results as (
            {{ elementary_internal.current_tests_run_results_query(days_back=days_back, invocation_id=invocation_id) }}
        ),

        test_results_with_ivocations_order as (
            select
                *,
                {{ elementary.datediff(elementary.cast_as_timestamp('tests.detected_at'), elementary.current_timestamp(), 'day') }} as days_diff
                {# current_tests_run_results_query sets dbt_test_unique_id to be test_unique_id #}
                row_number() over (partition by test_unique_id order by detected_at desc) as invocations_order
            from test_results
        )

        select 
            id,
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
            owners,
            tags,
            meta,
            test_results_query,
            other,
            test_name,
            test_params,
            severity,
            status,
            days_diff,
            invocations_order
        from test_results_with_ivocations_order
    {%- endset -%}
    {% set test_results_agate = run_query(select_test_results) %}
    {% set test_result_dicts_json = elementary.agate_to_json(test_results_agate) %}
    {% do elementary.edr_log(test_result_dicts_json) %}
{%- endmacro -%}

