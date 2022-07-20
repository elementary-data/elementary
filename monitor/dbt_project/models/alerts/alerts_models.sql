{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id'],
    on_schema_change = 'append_new_columns'
  )
}}


{% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
{% set error_models_relation = adapter.get_relation(elementary_database, elementary_schema, 'error_models') %}
{% if error_models_relation %}
    select *, false as alert_sent from {{ error_models_relation }}
    {% if is_incremental() %}
        where {{ get_new_alerts_where_clause(this) }}
    {% endif %}
{% endif %}
