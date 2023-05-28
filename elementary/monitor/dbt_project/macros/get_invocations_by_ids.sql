{% macro get_invocations_by_ids(ids) %}
  {% set database, schema = elementary.target_database(), target.schema %}
  {% set invocations_relation = adapter.get_relation(database, schema, 'dbt_invocations') %}
  {% if invocations_relation %}
    {% set get_invocations_query %}
      select
        invocation_id,
        command,
        selected,
        full_refresh,
        job_url,
        job_name,
        job_id,
        orchestrator
      from {{ invocations_relation }}
      where invocation_id in {{ elementary.strings_list_to_tuple(ids) }}
    {% endset %}
    {% set result = elementary.run_query(get_invocations_query) %}
    {% do return(elementary.agate_to_dicts(result)) %}
  {% endif %}
{% endmacro %}
