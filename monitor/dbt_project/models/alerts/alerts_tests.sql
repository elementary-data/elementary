{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id']
  )
}}

with alerts_schema_changes as (
    select * from {{ ref('elementary', 'alerts_schema_changes') }}
),

alerts_anomaly_detection as (
    select * from {{ ref('elementary', 'alerts_anomaly_detection') }}
),

alerts_dbt_tests as (
    select * from {{ ref('elementary', 'alerts_dbt_tests') }}
),

all_alerts as (
     select * from alerts_schema_changes
     union all
     select * from alerts_anomaly_detection
     union all
     select * from alerts_dbt_tests
)

select *, false as alert_sent
from all_alerts
{%- if is_incremental() %}
{%- set row_count = elementary.get_row_count(this) %}
    {%- if row_count > 0 %}
        where detected_at > (select max(detected_at) from {{ this }})
    {%- endif %}
{%- endif %}
