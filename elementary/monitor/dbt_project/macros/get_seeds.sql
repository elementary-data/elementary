{% macro get_seeds() %}
  {% set get_seeds_query %}
        with dbt_artifacts_seeds as (
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
          from {{ ref('elementary', 'dbt_seeds') }}
        )

        select * from dbt_artifacts_seeds
  {% endset %}

  {% set seeds_agate = run_query(get_seeds_query) %}
  {% do return(elementary.agate_to_dicts(seeds_agate)) %}
{% endmacro %}
