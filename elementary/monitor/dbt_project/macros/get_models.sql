{% macro get_models(exclude_elementary=false) %}
    {% set dbt_models_relation = ref('elementary', 'dbt_models') %}
    {%- if elementary.relation_exists(dbt_models_relation) -%}
        --{# TODO: should we group by #}
        {% set get_models_query %}
              with dbt_artifacts_models as (
                select
                  name,
                  unique_id,
                  database_name,
                  schema_name,
                  case when alias is not null then alias
                  else name end as table_name,
                  owner as owners,
                  tags,
                  package_name,
                  description,
                  original_path as full_path
                from {{ dbt_models_relation }}
                {% if exclude_elementary %}
                  where package_name != 'elementary'
                {% endif %}
              )

             select * from dbt_artifacts_models
        {% endset %}
        {% set models_agate = run_query(get_models_query) %}
        {% set models_json = elementary.agate_to_json(models_agate) %}
        {% do elementary.edr_log(models_json) %}
    {%- endif -%}
{% endmacro %}