{% macro get_snapshots() %}
  {% set get_snapshots_query %}
        with dbt_artifacts_snapshots as (
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
          from {{ ref('elementary', 'dbt_snapshots') }}
        )

        select * from dbt_artifacts_snapshots
  {% endset %}

  {% set snapshots_agate = run_query(get_snapshots_query) %}
  {% do return(elementary.agate_to_dicts(snapshots_agate)) %}
{% endmacro %}
