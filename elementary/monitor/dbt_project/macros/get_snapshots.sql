{% macro get_snapshots() %}
    {% set dbt_snapshots_relation = ref('elementary', 'dbt_snapshots') %}
    {%- if elementary.relation_exists(dbt_snapshots_relation) -%}
        {% set get_snapshots_query %}
            with dbt_artifacts_snapshots as (
                select 
                  name,
                  case when alias is not null then alias
                  else name end as table_name,
                  unique_id,
                  owner as owners,
                  tags,
                  package_name,
                  description,
                  meta,
                  materialization,
                  database_name,
                  schema_name,
                  depends_on_macros,
                  depends_on_nodes,
                  original_path as full_path,
                  path,
                  patch_path,
                  generated_at,
                  unique_key,
                  incremental_strategy
                from {{ dbt_snapshots_relation }}
              )
            select * from dbt_artifacts_snapshots
        {% endset %}

        {% set snapshots_agate = run_query(get_snapshots_query) %}
        {% do return(elementary.agate_to_dicts(snapshots_agate)) %}
    {%- endif -%}
{% endmacro %}