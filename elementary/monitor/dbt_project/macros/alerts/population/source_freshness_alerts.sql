{% macro populate_source_freshness_alerts_query() %}
    with dbt_source_freshness_results as (
      select * from {{ ref('dbt_source_freshness_results') }}
    ),

    dbt_sources as (
      select * from {{ ref('dbt_sources') }}
    )

    select
      results.source_freshness_execution_id as alert_id,
      results.max_loaded_at,
      results.snapshotted_at,
      {{ elementary.edr_cast_as_timestamp("results.generated_at") }} as detected_at,
      results.max_loaded_at_time_ago_in_s,
      results.status,
      results.error,
      results.warn_after,
      results.error_after,
      results.filter,
      sources.unique_id,
      sources.database_name,
      sources.schema_name,
      sources.source_name,
      sources.identifier,
      sources.tags,
      sources.meta,
      sources.owner,
      sources.package_name,
      sources.path,
      -- These columns below are deprecated. We add them since this view
      -- was used to be loaded into an incremental model with those columns, their names were later changed
      -- and Databricks doesn't respect `on_schema_change = 'append_new_columns'` properly, as described here -
      -- https://docs.databricks.com/en/delta/update-schema.html#automatic-schema-evolution-for-delta-lake-merge
      results.error_after as freshness_error_after,
      results.warn_after as freshness_warn_after,
      results.filter as freshness_filter
    from dbt_source_freshness_results as results
    join dbt_sources as sources
    on results.unique_id = sources.unique_id
    where lower(status) != 'pass'
{% endmacro %}
