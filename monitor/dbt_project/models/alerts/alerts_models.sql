{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id']
  )
}}

with alerts_dbt_models as (
    select * from {{ ref('elementary', 'alerts_dbt_models') }}
)

select *, false as alert_sent from alerts_dbt_models

{%- if is_incremental() %}
{%- set row_count = elementary.get_row_count(this) %}
    {%- if row_count > 0 %}
        where detected_at > (select max(detected_at) from {{ this }})
    {%- endif %}
{%- endif %}
