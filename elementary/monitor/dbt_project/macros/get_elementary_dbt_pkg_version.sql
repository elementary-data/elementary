{% macro get_elementary_dbt_pkg_version() %}
  {% set database, schema = elementary.target_database(), target.schema %}
  {% set invocations_relation = adapter.get_relation(database, schema, 'dbt_invocations') %}
  {% if not invocations_relation %}
    {% do elementary.edr_log('') %}
    {% do return(none) %}
  {% endif %}

  {% set get_pkg_version_query %}
    select elementary_version from {{ invocations_relation }} order by generated_at desc limit 1
  {% endset %}
  {% set result = dbt.run_query(get_pkg_version_query)[0][0] %}
  {% do elementary.edr_log(result or '') %}
{% endmacro %}
