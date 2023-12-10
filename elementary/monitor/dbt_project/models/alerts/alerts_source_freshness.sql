{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id'],
    on_schema_change = 'sync_all_columns',
    table_type=elementary.get_default_table_type(),
    incremental_strategy=elementary.get_default_incremental_strategy()
  )
}}

{% set alerts_source_freshness_relation = adapter.get_relation(this.database, this.schema, 'alerts_dbt_source_freshness') %}
{% if alerts_source_freshness_relation %}
    select
      {{ dbt_utils.star(from=alerts_source_freshness_relation, except=["detected_at"]) }},
      {{ elementary_cli.get_alerts_model_detected_at_expr(this) }} as detected_at,
      false as alert_sent,  {# backwards compatibility #}
      'pending' as suppression_status,
      {{ elementary.edr_cast_as_string('NULL') }} as sent_at
    from {{ alerts_source_freshness_relation }}
    {% if is_incremental() %}
        {{ elementary_cli.get_new_alerts_where_clause(this) }}
    {% endif %}
{% else %}
    {{ elementary_cli.empty_alerts_source_freshness() }}
{% endif %}
