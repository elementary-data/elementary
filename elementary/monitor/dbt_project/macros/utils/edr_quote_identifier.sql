{%- macro edr_quote_identifier(identifier) -%}
    {%- if elementary.is_tsql() -%}
        [{{ identifier }}]
    {%- else -%}
        {{ identifier }}
    {%- endif -%}
{%- endmacro -%}
