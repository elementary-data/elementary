{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id'],
    on_schema_change = 'append_new_columns'
  )
}}


{% set alerts_source_freshness_relation = adapter.get_relation(this.database, this.schema, 'alerts_dbt_source_freshness') %}
{% if alerts_source_freshness_relation %}
    select *, false as alert_sent from {{ alerts_source_freshness_relation }}
    {% if is_incremental() %}
        {{ get_new_alerts_where_clause(this) }}
    {% endif %}
{% else %}
    {{ empty_alerts_source_freshness() }}
{% endif %}
