{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id']
  )
}}


select *, false as alert_sent from {{ ref('elementary', 'failed_tests') }}

{%- if is_incremental() %}
    where {{ get_new_alerts_where_clause(this) }}
{%- endif %}
