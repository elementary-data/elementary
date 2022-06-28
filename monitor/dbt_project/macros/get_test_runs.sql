{%- macro get_test_runs(test_unique_id, table_name, column_name, test_type, test_sub_type) -%}
    {% set test_runs = none %}
    {% set test_id = "'" ~ test_unique_id ~ "'" %}
    {% set type = "'" ~ test_type ~ "'" %}
    {% set sub_type = "'" ~ test_sub_type ~ "'" %}
    {% set table_name = "'" ~ table_name ~ "'" %}
    {% set column_name = "'" ~ column_name ~ "'" %}
    {% set test_runs_query %}
        with elemetary_test_results as (
            select * from {{ ref('elementary', 'elementary_test_results') }}
        ),

        last_30_test_runs as (
            select *
            from elemetary_test_results
            where 
                test_unique_id = {{ test_id }}
                and test_type = {{ type }}
                and test_sub_type = {{ sub_type }}
                and table_name = {{ table_name }}
                and column_name = {{ column_name }}
            order by detected_at 
            limit 30
        )

        select 
            test_execution_id as id,
            detected_at as time_utc,
            case
                when test_type = 'dbt_test' and status = 'pass' then 0
                when test_type = 'dbt_test' and status != 'pass' then TO_NUMBER(SPLIT(test_results_description, ' ')[1])
                else null 
                end as affected_rows,
            status
        from last_30_test_runs
    {% endset %}
    {% set test_runs_agate = run_query(test_runs_query) %}
    {% set test_runs = elementary.agate_to_dicts(test_runs_agate) %}
    {{- return(test_runs) -}}
{%- endmacro -%}