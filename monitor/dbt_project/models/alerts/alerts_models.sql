{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id'],
    on_schema_change = 'append_new_columns'
  )
}}


select *, false as alert_sent from {{ ref('elementary', 'error_models') }}

{%- if is_incremental() %}
    where {{ get_new_alerts_where_clause(this) }}
{%- endif %}
