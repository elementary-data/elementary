{# We override the materialization in order to achieve schema migration mode without actually transforming the data. #}
{% materialization table, default -%}
  {% if not var("sync", false) %}
    {{ return(dbt.materialization_table_default()) }}
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
  {# We call the incremental materialization in order to hand-off the implementation of schema changes to dbt. #}
  {{ return(dbt.materialization_incremental_default()) }}
{% endmaterialization %}
