{% macro get_sub_test_unique_id(model_unique_id, test_unique_id, column_name, test_sub_type ) %}
    {% set sub_test_unique_id = model_unique_id ~ '.' ~ test_unique_id ~ '.' ~ column_name ~ '.' ~ test_sub_type %}
    {{ return(sub_test_unique_id) }}
{% endmacro %}