{% macro any_value(expression) -%}
    {{ return(adapter.dispatch('any_value', 'dbt_utils') (expression)) }}
{% endmacro %}


{% macro default__any_value(expression) -%}
    
    any_value({{ expression }})
    
{%- endmacro %}


{% macro postgres__any_value(expression) -%}
    {#- /*Postgres doesn't support any_value, so we're using min() to get the same result*/ -#}
    min({{ expression }})
    
{%- endmacro %}