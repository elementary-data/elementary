{%- macro get_tests_invocations(invocations_per_test = 30, days_back = 7) -%}
    {% set tests_invocations = {} %}
    {% set tests_invocations_query %}
        with elemetary_test_results as (
            select * from {{ ref('elementary', 'elementary_test_results') }}
        ),

        test_results_in_last_chosen_days as (
            select *
            from elemetary_test_results
            where {{ dbt_utils.datediff(elementary.cast_as_timestamp('detected_at'), dbt_utils.current_timestamp(), 'day') }} < {{ days_back }}
        )

        select
            model_unique_id, 
            test_unique_id,
            test_sub_type,
            column_name,
            SPLIT(
               {{ dbt_utils.listagg(measure='detected_at', delimiter_text="';@#;#@;'", order_by_clause="order by detected_at", limit_num=invocations_per_test)}},
               ';@#;#@;'
            ) as invocations_times,
            SPLIT(
              {{ dbt_utils.listagg(measure='test_results_description', delimiter_text="';@#;#@;'", order_by_clause="order by detected_at", limit_num=invocations_per_test)}},
              ';@#;#@;'
            ) as test_results_descriptions,
            SPLIT(
              {{ dbt_utils.listagg(measure='test_execution_id', delimiter_text="';@#;#@;'", order_by_clause="order by detected_at", limit_num=invocations_per_test)}},
              ';@#;#@;'
            ) as ids,
            SPLIT(
              {{ dbt_utils.listagg(measure='status', delimiter_text="';@#;#@;'", order_by_clause="order by detected_at", limit_num=invocations_per_test)}},
              ';@#;#@;'
            ) as statuses
        from test_results_in_last_chosen_days
        {{ dbt_utils.group_by(4) }}
    {% endset %}
    {% set tests_invocations_agate = run_query(tests_invocations_query) %}
    {% set tests_invocations_results = elementary.agate_to_dicts(tests_invocations_agate) %}
    {% for test in tests_invocations_results %}
        {% set sub_test_unique_id = get_sub_test_unique_id(
            model_unique_id=elementary.insensitive_get_dict_value(test, 'model_unique_id'),
            test_unique_id=elementary.insensitive_get_dict_value(test, 'test_unique_id'),
            test_sub_type=elementary.insensitive_get_dict_value(test, 'test_sub_type'),
            column_name=elementary.insensitive_get_dict_value(test, 'column_name'),
        ) %}
        {% do tests_invocations.update({sub_test_unique_id: test}) %}
    {% endfor %}
    {% do elementary.edr_log(tojson(tests_invocations)) %}
{%- endmacro -%}