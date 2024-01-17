{% macro get_adapter_type_and_unique_id() %}
    {% do return([target.type, adapter.dispatch('get_adapter_unique_id', 'elementary_cli')()]) %}
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

{% macro trino__get_adapter_unique_id() %}
    {{ exceptions.raise_compiler_error("Force failure and thus set warehouse_type=null since Trino is not currently supported within the report index.html") }}
{% endmacro %}
