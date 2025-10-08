{% macro test_adapter_specific_macros_have_default_implementation() %}
    {% set no_default_macros = [] %}
    {% set elementary_macros = elementary.keys() %}
    {% for macro in elementary_macros %}
        {% set parts = macro.split("__") %}
        {% if parts | length == 2 %}
            {% set adapter, macro_name = parts %}
            {% if macro_name not in no_default_macros and "default__{}".format(macro_name) not in elementary_macros %}
                {% do no_default_macros.append(macro_name) %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {{ assert_lists_contain_same_items(no_default_macros, [], "no_default_macros") }}
{% endmacro %}
