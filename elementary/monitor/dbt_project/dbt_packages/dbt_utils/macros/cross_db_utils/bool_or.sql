{% macro bool_or(expression) -%}
    {{ return(adapter.dispatch('bool_or', 'dbt_utils') (expression)) }}
{% endmacro %}


{% macro default__bool_or(expression) -%}
    
    bool_or({{ expression }})
    
{%- endmacro %}


{% macro snowflake__bool_or(expression) -%}
    
    boolor_agg({{ expression }})
    
{%- endmacro %}


{% macro bigquery__bool_or(expression) -%}
    
    logical_or({{ expression }})
    
{%- endmacro %}