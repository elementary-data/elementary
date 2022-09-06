{% macro listagg(measure, delimiter_text="','", order_by_clause=none, limit_num=none) -%}
    {{ return(adapter.dispatch('listagg', 'dbt_utils') (measure, delimiter_text, order_by_clause, limit_num)) }}
{%- endmacro %}

{% macro default__listagg(measure, delimiter_text, order_by_clause, limit_num) -%}

    {% if limit_num -%}
    array_to_string(
        array_slice(
            array_agg(
                {{ measure }}
            ){% if order_by_clause -%}
            within group ({{ order_by_clause }})
            {%- endif %}
            ,0
            ,{{ limit_num }}
        ),
        {{ delimiter_text }}
        )
    {%- else %}
    listagg(
        {{ measure }},
        {{ delimiter_text }}
        )
        {% if order_by_clause -%}
        within group ({{ order_by_clause }})
        {%- endif %}
    {%- endif %}

{%- endmacro %}

{% macro bigquery__listagg(measure, delimiter_text, order_by_clause, limit_num) -%}

    string_agg(
        {{ measure }},
        {{ delimiter_text }}
        {% if order_by_clause -%}
        {{ order_by_clause }}
        {%- endif %}
        {% if limit_num -%}
        limit {{ limit_num }}
        {%- endif %}
        )

{%- endmacro %}

{% macro postgres__listagg(measure, delimiter_text, order_by_clause, limit_num) -%}
    
    {% if limit_num -%}
    array_to_string(
        (array_agg(
            {{ measure }}
            {% if order_by_clause -%}
            {{ order_by_clause }}
            {%- endif %}
        ))[1:{{ limit_num }}],
        {{ delimiter_text }}
        )
    {%- else %}
    string_agg(
        {{ measure }},
        {{ delimiter_text }}
        {% if order_by_clause -%}
        {{ order_by_clause }}
        {%- endif %}
        )
    {%- endif %}

{%- endmacro %}

{# if there are instances of delimiter_text within your measure, you cannot include a limit_num #}
{% macro redshift__listagg(measure, delimiter_text, order_by_clause, limit_num) -%}

    {% if limit_num -%}
    {% set ns = namespace() %}
    {% set ns.delimiter_text_regex = delimiter_text|trim("'") %}
    {% set special_chars %}\,^,$,.,|,?,*,+,(,),[,],{,}{% endset %}  
    {%- for char in special_chars.split(',') -%}
        {% set escape_char %}\\{{ char }}{% endset %}
        {% set ns.delimiter_text_regex = ns.delimiter_text_regex|replace(char,escape_char) %}
    {%- endfor -%}

    {% set regex %}'([^{{ ns.delimiter_text_regex }}]+{{ ns.delimiter_text_regex }}){1,{{ limit_num - 1}}}[^{{ ns.delimiter_text_regex }}]+'{% endset %}
    regexp_substr(
        listagg(
            {{ measure }},
            {{ delimiter_text }}
            )
            {% if order_by_clause -%}
            within group ({{ order_by_clause }})
            {%- endif %}
        ,{{ regex }}
        )
    {%- else %}
    listagg(
        {{ measure }},
        {{ delimiter_text }}
        )
        {% if order_by_clause -%}
        within group ({{ order_by_clause }})
        {%- endif %}
    {%- endif %}

{%- endmacro %}