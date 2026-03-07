{%- macro edr_quote_identifier(identifier) -%}
    {%- if target.type in ('fabric', 'sqlserver') -%}
        [{{ identifier }}]
    {%- else -%}
        {{ identifier }}
    {%- endif -%}
{%- endmacro -%}
