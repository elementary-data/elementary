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
{% set alerts_relation = adapter.get_relation(this.database, this.schema, 'alerts') %}

with all_tests as (
    select *
    from {{ ref('elementary', 'alerts_dbt_tests') }}
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

, failed_tests as (
    select * from all_tests
    where status != 'pass'
)

{% if alerts_relation %}
, latest_alert_status as (
    SELECT model_unique_id, test_name, status, detected_at
    FROM (
        SELECT model_unique_id, test_name, status, detected_at,
            ROW_NUMBER() OVER (PARTITION BY model_unique_id, test_name ORDER BY detected_at DESC) AS rn
        FROM {{ alerts_relation }}
    ) ranked
    WHERE rn = 1
)
, resolved_tests as (
    select current_alerts.* 
    from all_tests as current_alerts
    join latest_alert_status as latest_alerts
        on current_alerts.model_unique_id = latest_alerts.model_unique_id
        and current_alerts.test_name = latest_alerts.test_name
    where current_alerts.status = 'pass'
        and latest_alerts.status != 'pass'
  )
{% endif %}

select 
    *,
    false as alert_sent,  {# backwards compatibility #}
    'pending' as suppression_status,
    {{ elementary.edr_cast_as_string('NULL') }} as sent_at
from (
    select * from failed_tests
    {% if alerts_relation %}
    union all
    select * from resolved_tests
    {% endif %}
) as all_tests

{%- if is_incremental() %}
    {{ get_new_alerts_where_clause(this) }}
{%- endif %}
