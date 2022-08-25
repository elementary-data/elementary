{% macro get_sources() %}
    {% set sources_dict = {} %}
    {% set dbt_sources_relation = ref('elementary', 'dbt_sources') %}
    {%- if elementary.relation_exists(dbt_sources_relation) -%}
        --{# TODO: should we group by #}
        {% set get_sources_query %}
            with dbt_artifacts_sources as (
                select
                  name,
                  unique_id,
                  database_name,
                  schema_name,
                  identifier as table_name,
                  owner as owners,
                  tags,
                  package_name,
                  description,
                  original_path as full_path
                from {{ dbt_sources_relation }}
              )

            select * from dbt_artifacts_sources
        {% endset %}
        {% set sources_agate = run_query(get_sources_query) %}
        {% set sources_json = elementary.agate_to_json(sources_agate) %}
        {% do elementary.edr_log(sources_json) %}
    {%- endif -%}
{% endmacro %}