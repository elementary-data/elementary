{% macro get_recommended_tests(where_expression) %}
    {% if not where_expression %}
        {% do exceptions.raise_compiler_error("A 'where_expression' argument is required.") %}
    {% endif %}

    {% set query %}
        select resource_name, source_name, test_namespace, test_name, test_args, table_args
        from {{ ref("test_recommendations") }}
        where {{ where_expression }}
    {% endset %}

    {% set result = elementary.run_query(query) %}
    {% do return(elementary.agate_to_dicts(result)) %}
{% endmacro %}
