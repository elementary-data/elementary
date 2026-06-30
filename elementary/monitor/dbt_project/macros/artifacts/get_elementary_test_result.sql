{%- macro get_elementary_test_result(test_result_id) -%}
    {% set relation = ref('elementary', 'elementary_test_results') %}
    {% if not elementary.relation_exists(relation) %}
        {% do return([]) %}
    {% endif %}

    {% set query %}
        select
            id,
            test_execution_id,
            test_unique_id,
            model_unique_id,
            detected_at,
            database_name,
            schema_name,
            table_name,
            column_name,
            test_type,
            test_sub_type,
            test_name,
            test_short_name,
            severity,
            status,
            test_results_description,
            failures,
            failed_row_count
        from {{ relation }}
        where id = '{{ elementary_cli._sql_escape(test_result_id) }}'
        limit 1
    {% endset %}

    {% set results = elementary.run_query(query) %}
    {% do return(elementary.agate_to_dicts(results)) %}
{%- endmacro -%}
