{% macro log_macro_results(macro_name, macro_args = {}) %}
    {% set results = context[macro_name](**macro_args) %}
    {% do elementary.edr_log(tojson(results)) %}
{% endmacro %}