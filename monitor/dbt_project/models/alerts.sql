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

alerts_data_monitoring as (

    select * from {{ ref('elementary', 'alerts_data_monitoring') }}

),

all_alerts as (

     select * from alerts_schema_changes
     union all
     select * from alerts_data_monitoring

)

select *, false as alert_sent
from all_alerts
{%- if is_incremental() %}
{%- set row_count = elementary.get_row_count(this) %}
    {%- if row_count > 0 %}
        where detected_at > (select max(detected_at) from {{ this }})
    {%- endif %}
{%- endif %}
