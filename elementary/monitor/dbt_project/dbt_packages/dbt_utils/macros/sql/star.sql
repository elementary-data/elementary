{% macro star(from, relation_alias=False, except=[], prefix='', suffix='') -%}
    {{ return(adapter.dispatch('star', 'dbt_utils')(from, relation_alias, except, prefix, suffix)) }}
{% endmacro %}

{% macro default__star(from, relation_alias=False, except=[], prefix='', suffix='') -%}
    {%- do dbt_utils._is_relation(from, 'star') -%}
    {%- do dbt_utils._is_ephemeral(from, 'star') -%}

    {#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {%- if not execute -%}
        {{ return('*') }}
    {% endif %}

    {% set cols = dbt_utils.get_filtered_columns_in_relation(from, except) %}

    {%- if cols|length <= 0 -%}
      {{- return('*') -}}
    {%- else -%}
        {%- for col in cols %}
            {%- if relation_alias %}{{ relation_alias }}.{% else %}{%- endif -%}{{ adapter.quote(col)|trim }} {%- if prefix!='' or suffix!='' %} as {{ adapter.quote(prefix ~ col ~ suffix)|trim }} {%- endif -%}
            {%- if not loop.last %},{{ '\n  ' }}{% endif %}
        {%- endfor -%}
    {% endif %}
{%- endmacro %}
