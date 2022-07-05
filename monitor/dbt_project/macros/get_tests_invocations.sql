{%- macro get_tests_invocations(invocations_per_test = 30) -%}
    {% set tests_invocations = {} %}
    {% set tests_invocations_query %}
        with elemetary_test_results as (
            select * from {{ ref('elementary', 'elementary_test_results') }}
        ),
        
        
        elementary_test_results_with_affected_rows as (
            select 
                *,
                case
                    when test_type = 'dbt_test' and status = 'pass' then 0
                    when test_type = 'dbt_test' and status != 'pass' then TO_NUMBER(SPLIT(test_results_description, ' ')[1])
                    else null 
                end as affected_rows
            from elemetary_test_results
        )

        select 
            test_unique_id,
            test_type,
            test_sub_type,
            table_name,
            column_name,
            SPLIT(
               {{ dbt_utils.listagg(measure='detected_at', delimiter_text="';;;'", order_by_clause="order by detected_at", limit_num=invocations_per_test)}},
               ';;;'
            ) as invocations_times,
            SPLIT(
              {{ dbt_utils.listagg(measure='affected_rows', delimiter_text="';;;'", order_by_clause="order by detected_at", limit_num=invocations_per_test)}},
              ';;;'
            ) as affected_rows,
            SPLIT(
              {{ dbt_utils.listagg(measure='test_execution_id', delimiter_text="';;;'", order_by_clause="order by detected_at", limit_num=invocations_per_test)}},
              ';;;'
            ) as ids,
            SPLIT(
              {{ dbt_utils.listagg(measure='status', delimiter_text="';;;'", order_by_clause="order by detected_at", limit_num=invocations_per_test)}},
              ';;;'
            ) as statuses
        from elementary_test_results_with_affected_rows
        group by 
            test_unique_id,
            test_type,
            test_sub_type,
            table_name,
            column_name
    {% endset %}
    {% set tests_invocations_agate = run_query(tests_invocations_query) %}
    {% set tests_invocations_results = elementary.agate_to_dicts(tests_invocations_agate) %}
    {% for test in tests_invocations_results %}
        {% set sub_test_unique_id = get_sub_test_unique_id(
            test_unique_id=elementary.insensitive_get_dict_value(test, 'test_unique_id'),
            test_type=elementary.insensitive_get_dict_value(test, 'test_type'),
            test_sub_type=elementary.insensitive_get_dict_value(test, 'test_sub_type'),
            table_name=elementary.insensitive_get_dict_value(test, 'table_name'),
            column_name=elementary.insensitive_get_dict_value(test, 'column_name'),
        ) %}
        {% do tests_invocations.update({sub_test_unique_id: test}) %}
    {% endfor %}
    {% do elementary.edr_log(tojson(tests_invocations)) %}
{%- endmacro -%}