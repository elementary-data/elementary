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
    {{ get_new_alerts_where_clause(this) }}
{%- endif %}
