{% macro populate_source_freshness_alerts(days_back=1) %}
    {% set source_freshness_alerts = [] %}
    {% set raw_source_freshness_alerts_agate = run_query(elementary_cli.populate_source_freshness_alerts_query(days_back)) %}
    {% set raw_source_freshness_alerts = elementary.agate_to_dicts(raw_source_freshness_alerts_agate) %}
    {% for raw_source_freshness_alert in raw_source_freshness_alerts %}
        {% set error_after = raw_source_freshness_alert.get('error_after') %}
        {% set warn_after = raw_source_freshness_alert.get('warn_after') %}
        {% set filter = raw_source_freshness_alert.get('filter') %}

        {# we want to use the normalized status for all usages #}
        {% set status = raw_source_freshness_alert.get('normalized_status') %}

        {% set source_freshness_alert_data = {
          'id':  raw_source_freshness_alert.get('alert_id'),
          'source_freshness_execution_id': raw_source_freshness_alert.get('source_freshness_execution_id'),
          'model_unique_id': raw_source_freshness_alert.get('unique_id'),
          'alert_class_id': raw_source_freshness_alert.get('alert_class_id'),
          'detected_at': raw_source_freshness_alert.get('detected_at'),
          'snapshotted_at': raw_source_freshness_alert.get('snapshotted_at'),
          'max_loaded_at': raw_source_freshness_alert.get('max_loaded_at'),
          'max_loaded_at_time_ago_in_s': raw_source_freshness_alert.get('max_loaded_at_time_ago_in_s'),
          'database_name': raw_source_freshness_alert.get('database_name'),
          'schema_name': raw_source_freshness_alert.get('schema_name'),
          'source_name': raw_source_freshness_alert.get('source_name'),
          'identifier': raw_source_freshness_alert.get('identifier'),
          'error_after': error_after if error_after is not none else  raw_source_freshness_alert.get('freshness_error_after'),
          'warn_after': warn_after if warn_after is not none else  raw_source_freshness_alert.get('freshness_warn_after'),
          'filter': filter if filter is not none else  raw_source_freshness_alert.get('freshness_filter'),
          'status': status,
          'original_status': raw_source_freshness_alert.get('original_status'),
          'owners': raw_source_freshness_alert.get('owner'),
          'path': raw_source_freshness_alert.get('path'),
          'error': raw_source_freshness_alert.get('error'),
          'tags': raw_source_freshness_alert.get('tags'),
          'model_meta': raw_source_freshness_alert.get('model_meta'),
          'freshness_description': raw_source_freshness_alert.get('freshness_description')
        } %}

        {% set source_freshness_alert = elementary_cli.generate_alert_object(
            elementary.insensitive_get_dict_value(raw_source_freshness_alert, 'alert_id'),
            elementary.insensitive_get_dict_value(raw_source_freshness_alert, 'alert_class_id'),
            'source_freshness',
            elementary.insensitive_get_dict_value(raw_source_freshness_alert, 'detected_at'),
            elementary.insensitive_get_dict_value(raw_source_freshness_alert, 'created_at'),
            source_freshness_alert_data,
        ) %}
        {% do source_freshness_alerts.append(source_freshness_alert) %}
    {% endfor %}
    {% do return(source_freshness_alerts) %}
{% endmacro %}


{% macro populate_source_freshness_alerts_query(days_back=1) %}
  {% set source_freshness_results_relation = ref('elementary', 'dbt_source_freshness_results') %}
  {% set error_after_column_exists = elementary.column_exists_in_relation(source_freshness_results_relation, 'error_after') %}

  {% set sources_relation = ref('elementary', 'dbt_sources') %}
  {% set freshness_description_column_exists = elementary.column_exists_in_relation(sources_relation, 'freshness_description') %}

  with dbt_source_freshness_results as (
    select * from {{ source_freshness_results_relation }}
  ),

  dbt_sources as (
    select * from {{ sources_relation }}
  ),

  source_freshness_alerts as (
    select
      results.source_freshness_execution_id as alert_id,
      {# Currently alert_class_id equals to unique_id - might change in the future so we return both #}
      results.unique_id as alert_class_id,
      results.unique_id,
      results.source_freshness_execution_id,
      results.max_loaded_at,
      results.snapshotted_at,
      {{ elementary.edr_cast_as_timestamp("results.generated_at") }} as detected_at,
      {{ elementary.edr_current_timestamp() }} as created_at,
      results.max_loaded_at_time_ago_in_s,
      results.status as original_status,
      {{ elementary_cli.normalized_source_freshness_status()}},
      {# backwards compatibility - these fields were added together #}
      {% if error_after_column_exists %}
        results.error_after,
        results.warn_after,
        results.filter,
      {% endif %}
      results.error,
      sources.database_name,
      sources.schema_name,
      sources.source_name,
      sources.identifier,
      sources.tags,
      sources.meta,
      sources.owner,
      sources.package_name,
      sources.path,
      {% if freshness_description_column_exists %}
        sources.freshness_description,
      {% else %}
        'dbt source freshness validates if the data in a table is not updated by calculating if the time elapsed between the test execution to the latest record is above an acceptable SLA threshold.' as freshness_description,
      {% endif %}
      sources.meta as model_meta,
      sources.freshness_error_after,
      sources.freshness_warn_after,
      sources.freshness_filter
    from dbt_source_freshness_results as results
    join dbt_sources as sources
    on results.unique_id = sources.unique_id
    where lower(status) != 'pass'
    and {{ elementary.edr_cast_as_timestamp('results.generated_at') }} > {{ elementary.edr_timeadd('day', -1 * days_back, elementary.edr_current_timestamp()) }}
  )

  select *
  from source_freshness_alerts
  where source_freshness_alerts.alert_id not in (
    {# "this" is referring to "alerts_v2" - we are executing it using a post_hook over "alerts_v2" #}
    select alert_id from {{ this }}
  )
{% endmacro %}
