{% macro dummy_macro() %}
  {# Validate that logs in the macro don't fail the dbt runner #}
  {% do elementary.edr_log('Amazing macro, so happy to be here') %}
  {% do return({'goodbye': 'toodleoo'}) %}
{% endmacro %}
