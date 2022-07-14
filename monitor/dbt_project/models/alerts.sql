{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id']
  )
}}

{% set last_alert_time_with_backfill_query %}
    {# We use a backfill of 2 days to prevent race condition when multiple dbt tests + edr monitor run together #}
    select {{ elementary.timeadd('day', '-2', 'max(detected_at)') }} from {{ this }}
{% endset %}

{% set last_alert_time_with_backfill = elementary.result_value(last_alert_time_with_backfill_query) %}

with alerts_schema_changes as (
    select * from {{ ref('elementary', 'alerts_schema_changes') }}
),

alerts_data_monitoring as (
    select * from {{ ref('elementary', 'alerts_data_monitoring') }}
),

alerts_dbt_tests as (
    select * from {{ ref('elementary', 'alerts_dbt_tests') }}
),

all_alerts as (
     select * from alerts_schema_changes
     union all
     select * from alerts_data_monitoring
     union all
     select * from alerts_dbt_tests
)

select *, false as alert_sent
from all_alerts
{%- if is_incremental() %}
{%- set row_count = elementary.get_row_count(this) %}
    {%- if row_count > 0 %}
        where detected_at > {{ last_alert_time_with_backfill }}
        and alert_id not in (select alert_id from {{this}} where detected_at > {{ last_alert_time_with_backfill }} alert_sent = true)
    {%- endif %}
{%- endif %}
