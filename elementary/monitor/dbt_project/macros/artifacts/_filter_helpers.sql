{#
    Helpers for building parameterized WHERE clauses in artifacts macros.
    Values come from the edr CLI — we still escape single quotes for hygiene
    before embedding into SQL.
#}

{%- macro _sql_escape(value) -%}
{{ value | string | replace("'", "''") }}
{%- endmacro -%}

{%- macro _eq_filter(column, value) -%}
    {%- if value is not none -%}
        and {{ column }} = '{{ elementary_cli._sql_escape(value) }}'
    {%- endif -%}
{%- endmacro -%}

{%- macro _eq_ci_filter(column, value) -%}
    {%- if value is not none -%}
        and lower({{ column }}) = lower('{{ elementary_cli._sql_escape(value) }}')
    {%- endif -%}
{%- endmacro -%}

{%- macro _like_ci_filter(column, value) -%}
    {%- if value is not none -%}
        and lower({{ column }}) like lower('%{{ elementary_cli._sql_escape(value) }}%')
    {%- endif -%}
{%- endmacro -%}

{%- macro _bool_filter(column, value) -%}
    {%- if value is not none -%}
        and {{ column }} = {{ 'true' if value else 'false' }}
    {%- endif -%}
{%- endmacro -%}

{%- macro _in_filter(column, values) -%}
    {%- if values is not none and values | length > 0 -%}
        and {{ column }} in (
            {%- for v in values -%}
                '{{ elementary_cli._sql_escape(v) }}'{% if not loop.last %}, {% endif %}
            {%- endfor -%}
        )
    {%- endif -%}
{%- endmacro -%}

{%- macro _gte_filter(column, value) -%}
    {%- if value is not none -%}
        and {{ column }} >= '{{ elementary_cli._sql_escape(value) }}'
    {%- endif -%}
{%- endmacro -%}

{%- macro _lte_filter(column, value) -%}
    {%- if value is not none -%}
        and {{ column }} <= '{{ elementary_cli._sql_escape(value) }}'
    {%- endif -%}
{%- endmacro -%}

{%- macro _gt_number_filter(column, value) -%}
    {%- if value is not none -%}
        and {{ column }} > {{ value }}
    {%- endif -%}
{%- endmacro -%}

{%- macro _lt_number_filter(column, value) -%}
    {%- if value is not none -%}
        and {{ column }} < {{ value }}
    {%- endif -%}
{%- endmacro -%}
