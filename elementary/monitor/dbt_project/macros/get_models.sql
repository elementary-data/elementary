{% macro get_models(exclude_elementary=false) %}
    {% set dbt_models_relation = ref('elementary', 'dbt_models') %}
    {%- if elementary.relation_exists(dbt_models_relation) -%}
        {% set patch_path_column_exists = elementary.column_exists_in_relation(dbt_models_relation, 'patch_path') %}

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
                  materialization,
                  {# backwards compatibility #}
                  {% if patch_path_column_exists %}
                    patch_path,
                  {% endif %}
                  original_path as full_path,
                  meta
                from {{ dbt_models_relation }}
                {% if exclude_elementary %}
                  where package_name != 'elementary'
                {% endif %}
              )

             select * from dbt_artifacts_models
        {% endset %}
        {% set models_agate = run_query(get_models_query) %}
        {% do return(elementary.agate_to_dicts(models_agate)) %}
    {%- endif -%}
{% endmacro %}
