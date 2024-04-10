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

{% macro athena__get_adapter_unique_id() %}
    {{ return(target.s3_staging_dir) }}
{% endmacro %}
