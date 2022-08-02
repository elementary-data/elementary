{% macro get_adapter_unique_id() %}
    {{ elementary.edr_log(adapter.dispatch('get_adapter_unique_id')()) }}
{%- endmacro %}

{% macro default__get_adapter_unique_id() %}
    {{ return(target.host) }}
{% endmacro %}

{% macro snowflake__get_adapter_unique_id() %}
    {{ return(target.account) }}
{% endmacro %}

{% macro bigquery__get_adapter_unique_id() %}
    {{ return(target.project) }}
{% endmacro %}