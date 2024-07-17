{% macro log_macro_results(macro_name, macro_args=none) %}
    {% if macro_args is none %}
        {% set macro_args = {} %}
    {% endif %}
    {%- set package_and_macro_name = macro_name.split('.') %}
    {%- if package_and_macro_name | length == 1 %}
        {% set macro = context[macro_name] %}
    {%- elif package_and_macro_name | length == 2 %}
        {%- set package_name, macro_name = package_and_macro_name %}
        {% set macro = context[package_name][macro_name] %}
    {%- else %}
        {% do exceptions.raise_compiler_error("Received invalid macro name: {}".format(macro_name)) %}
    {% endif %}
    {% set results = macro(**macro_args) %}
    {% if results is not none %}
        {% do elementary.edr_log('--ELEMENTARY-MACRO-OUTPUT-START--' ~ tojson(results) ~ '--ELEMENTARY-MACRO-OUTPUT-END--') %}
    {% endif %}
{% endmacro %}
