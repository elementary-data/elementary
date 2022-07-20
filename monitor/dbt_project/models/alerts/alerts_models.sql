{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id'],
    on_schema_change = 'append_new_columns'
  )
}}


{% set error_models_relation = adapter.get_relation(this.database, this.schema, 'error_models') %}
{% if error_models_relation %}
    select *, false as alert_sent from {{ error_models_relation }}
    {% if is_incremental() %}
        where {{ get_new_alerts_where_clause(this) }}
    {% endif %}
{% endif %}
