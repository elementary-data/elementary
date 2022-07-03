{% macro get_sub_test_unique_id(test_unique_id, test_type, test_sub_type, table_name, column_name) %}
    {% set sub_test_unique_id = test_unique_id ~ '.' ~ test_type ~ '.' ~ test_sub_type ~ '.' ~ table_name ~ '.' ~ column_name %}
    {{ return(sub_test_unique_id) }}
{% endmacro %}