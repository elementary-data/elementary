{%- macro get_elementary_test_results(
    test_unique_id=none,
    model_unique_id=none,
    test_type=none,
    test_sub_type=none,
    test_name=none,
    status=none,
    table_name=none,
    column_name=none,
    database_name=none,
    schema_name=none,
    severity=none,
    detected_after=none,
    detected_before=none,
    limit=200
) -%}
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
        where 1=1
        {{ elementary_cli._eq_filter('test_unique_id', test_unique_id) }}
        {{ elementary_cli._eq_filter('model_unique_id', model_unique_id) }}
        {{ elementary_cli._eq_ci_filter('test_type', test_type) }}
        {{ elementary_cli._eq_ci_filter('test_sub_type', test_sub_type) }}
        {{ elementary_cli._like_ci_filter('test_name', test_name) }}
        {{ elementary_cli._eq_ci_filter('status', status) }}
        {{ elementary_cli._like_ci_filter('table_name', table_name) }}
        {{ elementary_cli._eq_ci_filter('column_name', column_name) }}
        {{ elementary_cli._eq_ci_filter('database_name', database_name) }}
        {{ elementary_cli._eq_ci_filter('schema_name', schema_name) }}
        {{ elementary_cli._eq_ci_filter('severity', severity) }}
        {{ elementary_cli._gte_filter('detected_at', detected_after) }}
        {{ elementary_cli._lte_filter('detected_at', detected_before) }}
        order by detected_at desc
        limit {{ limit }}
    {% endset %}

    {% set results = elementary.run_query(query) %}
    {% do return(elementary.agate_to_dicts(results)) %}
{%- endmacro -%}
