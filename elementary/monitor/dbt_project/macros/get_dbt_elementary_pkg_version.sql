{% macro get_elementary_dbt_pkg_version() %}
  {% set database, schema = elementary.target_database(), target.schema %}
  {% set information_relation = adapter.get_relation(database, schema, 'information') %}
  {% if not information_relation %}
    {% do elementary.edr_log(none) %}
    {% do return(none) %}
  {% endif %}

  {% set get_pkg_version_query %}
    select value from {{ information_relation }} where key = 'elementary_version'
  {% endset %}
  {% set output = dbt.run_query(get_pkg_version_query) %}
  {% do elementary.edr_log(output[0][0]) %}
{% endmacro %}
