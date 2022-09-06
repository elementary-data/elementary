{% macro split_part(string_text, delimiter_text, part_number) %}
  {{ return(adapter.dispatch('split_part', 'dbt_utils') (string_text, delimiter_text, part_number)) }}
{% endmacro %}


{% macro default__split_part(string_text, delimiter_text, part_number) %}

    split_part(
        {{ string_text }},
        {{ delimiter_text }},
        {{ part_number }}
        )

{% endmacro %}


{% macro _split_part_negative(string_text, delimiter_text, part_number) %}

    split_part(
        {{ string_text }},
        {{ delimiter_text }},
          length({{ string_text }}) 
          - length(
              replace({{ string_text }},  {{ delimiter_text }}, '')
          ) + 2 {{ part_number }}
        )

{% endmacro %}


{% macro postgres__split_part(string_text, delimiter_text, part_number) %}

  {% if part_number >= 0 %}
    {{ dbt_utils.default__split_part(string_text, delimiter_text, part_number) }}
  {% else %}
    {{ dbt_utils._split_part_negative(string_text, delimiter_text, part_number) }}
  {% endif %}

{% endmacro %}


{% macro redshift__split_part(string_text, delimiter_text, part_number) %}

  {% if part_number >= 0 %}
    {{ dbt_utils.default__split_part(string_text, delimiter_text, part_number) }}
  {% else %}
    {{ dbt_utils._split_part_negative(string_text, delimiter_text, part_number) }}
  {% endif %}

{% endmacro %}


{% macro bigquery__split_part(string_text, delimiter_text, part_number) %}

  {% if part_number >= 0 %}
    split(
        {{ string_text }},
        {{ delimiter_text }}
        )[safe_offset({{ part_number - 1 }})]
  {% else %}
    split(
        {{ string_text }},
        {{ delimiter_text }}
        )[safe_offset(
          length({{ string_text }}) 
          - length(
              replace({{ string_text }},  {{ delimiter_text }}, '')
          ) + 1
        )]
  {% endif %}

{% endmacro %}
