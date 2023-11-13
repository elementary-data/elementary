{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id'],
    on_schema_change = 'append_new_columns',
    table_type=elementary.get_default_table_type(),
    incremental_strategy=elementary.get_default_incremental_strategy()
  )
}}


{% set error_models_relation = adapter.get_relation(this.database, this.schema, 'alerts_dbt_models') %}
{% if error_models_relation %}
    select
      {{ dbt_utils.star(from=error_models_relation, except=["detected_at"]) }},
      {{ elementary.edr_cast_as_timestamp("detected_at") }} as detected_at,
      false as alert_sent,  {# backwards compatibility #}
      'pending' as suppression_status,
      {{ elementary.edr_cast_as_string('NULL') }} as sent_at
    from {{ error_models_relation }}
    {% if is_incremental() %}
        {{ elementary_cli.get_new_alerts_where_clause(this) }}
    {% endif %}
{% else %}
    {{ elementary_cli.empty_alerts_models() }}
{% endif %}
