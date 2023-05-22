{% macro get_invocations_by_ids(ids) %}
  {% set database, schema = elementary.target_database(), target.schema %}
  {% set invocations_relation = adapter.get_relation(database, schema, 'dbt_invocations') %}
  {% if not invocations_relation %}
    {% do elementary.edr_log('failed getting invocations relation') %}
    {% do return(none) %}
  {% endif %}

  {% set get_invocations_query %}
    select * from {{ invocations_relation }} where invocation_id in {{ elementary.strings_list_to_tuple(ids) }}
  {% endset %}
  {% set result = elementary.run_query(get_invocations_query) %}
  {% if not result %}
    {% do elementary.edr_log('no invocations were found') %}
    {% do return(none) %}
  {% endif %}

  {% do return(elementary.agate_to_dicts(result)) %}
{% endmacro %}
