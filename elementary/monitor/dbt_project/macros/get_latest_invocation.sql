{% macro get_latest_invocation() %}
  {% set database, schema = elementary.target_database(), target.schema %}
  {% set invocations_relation = adapter.get_relation(database, schema, 'dbt_invocations') %}
  {% if not invocations_relation %}
    {% do elementary.edr_log('') %}
    {% do return(none) %}
  {% endif %}

  {% set get_pkg_version_query %}
    select * from {{ invocations_relation }} order by generated_at desc limit 1
  {% endset %}
  {% set result = elementary.run_query(get_pkg_version_query) %}
  {% if not result %}
    {% do elementary.edr_log('') %}
    {% do return(none) %}
  {% endif %}

  {% do return(elementary.agate_to_dicts(result)) %}
{% endmacro %}
