{% macro escape_single_quotes(expression) %}
      {{ return(adapter.dispatch('escape_single_quotes', 'dbt_utils') (expression)) }}
{% endmacro %}

{# /*Default to replacing a single apostrophe with two apostrophes: they're -> they''re*/ #}
{% macro default__escape_single_quotes(expression) -%}
{{ expression | replace("'","''") }}
{%- endmacro %}

{# /*Snowflake uses a single backslash: they're -> they\'re. The second backslash is to escape it from Jinja */ #}
{% macro snowflake__escape_single_quotes(expression) -%}
{{ expression | replace("'", "\\'") }}
{%- endmacro %}

{# /*BigQuery uses a single backslash: they're -> they\'re. The second backslash is to escape it from Jinja */ #}
{% macro bigquery__escape_single_quotes(expression) -%}
{{ expression | replace("'", "\\'") }}
{%- endmacro %}
