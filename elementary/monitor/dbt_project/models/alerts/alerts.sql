{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id'],
    on_schema_change = 'append_new_columns'
  )
}}

{% set anomaly_detection_relation = adapter.get_relation(this.database, this.schema, 'alerts_anomaly_detection') %}
{# Backwards compatibility support for a renamed model. #}
{% set data_monitoring_relation = adapter.get_relation(this.database, this.schema, 'alerts_data_monitoring') %}
{% set schema_changes_relation = adapter.get_relation(this.database, this.schema, 'alerts_schema_changes') %}

with failed_tests as (
     select * from {{ ref('elementary', 'alerts_dbt_tests') }}

    {% if schema_changes_relation %}
        union all
        select * from {{ schema_changes_relation }}
    {% endif %}

    {% if anomaly_detection_relation %}
        union all
        select * from {{ anomaly_detection_relation }}
    {% elif data_monitoring_relation %}
        union all
        select * from {{ data_monitoring_relation }}
    {% endif %}
)

select *, false as alert_sent from failed_tests

{%- if is_incremental() %}
    {{ get_new_alerts_where_clause(this) }}
{%- endif %}
