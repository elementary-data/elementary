
{% macro assert_empty_table(table, context='') %}
    {% if table | length > 0 %}
        {% do elementary.edr_log(context ~ " FAILED: Table not empty.") %}
        {% do table.print_table() %}
        {{ return(1) }}
    {% endif %}
    {% do elementary.edr_log(context ~ " SUCCESS: Table is empty.") %}
    {{ return(0) }}
{% endmacro %}

{% macro assert_table_doesnt_exist(model_name) %}
    {% if load_relation(ref(model_name)) is none %}
        {% do elementary.edr_log(model_name ~ " SUCCESS: Table doesn't exist.") %}
        {{ return(0) }}
    {% endif %}
    {% do elementary.edr_log(context ~ " FAILED: Table exists.") %}
    {{ return(1) }}
{% endmacro %}