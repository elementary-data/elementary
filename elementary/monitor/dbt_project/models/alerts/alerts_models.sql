{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id'],
    on_schema_change = 'append_new_columns'
  )
}}


{% set error_models_relation = adapter.get_relation(this.database, this.schema, 'alerts_dbt_models') %}
{% if error_models_relation %}
    select 
      *,
      false as alert_sent,  {# backwards compatibility #}
      'pending' as suppression_status,
      {{ elementary.edr_cast_as_string('NULL') }} as sent_at
    from {{ error_models_relation }}
    {% if is_incremental() %}
        {{ get_new_alerts_where_clause(this) }}
    {% endif %}
{% else %}
    {{ empty_alerts_models() }}
{% endif %}
