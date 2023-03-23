{# We override the materialization in order to achieve schema migration mode without actually transforming the data. #}
{% materialization incremental, default -%}
  {% if not var("sync", false) %}
    {{ return(dbt.materialization_incremental_default()) }}
  {% endif %}

  {% set init_schema_sql %}
    with result as (
      {{ sql }}
    )
    select * from result where 1 = 0
  {% endset %}
  {% do context.update({
    "sql": init_schema_sql,
    "pre_hooks": [],
    "post_hooks": [],
    }) %}
  {{ return(dbt.materialization_incremental_default()) }}
{% endmaterialization %}
