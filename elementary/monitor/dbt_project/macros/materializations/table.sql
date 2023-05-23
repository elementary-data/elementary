{# We override the materialization in order to achieve schema migration mode without actually transforming the data. #}
{% materialization table, default -%}
  {% if not var("sync", false) %}
    {{ return(dbt.materialization_table_default()) }}
  {% endif %}

  {% set existing_relation = load_cached_relation(this) %}
  {% if not existing_relation %}
    {% set init_schema_sql %}
      with result as (
        {{ sql }}
      )
      select * from result where 1 = 0
    {% endset %}
    {% do context.update({"sql": init_schema_sql}) %}
  {# This is meant to deal with a race between initializing the table and loading data into it. #}
  {% else %}
    {% set same_sql %}
      select * from {{ this }}
    {% endset %}
    {% do context.update({"sql": same_sql}) %}
  {% endif %}

  {% do context.update({"pre_hooks": [], "post_hooks": []}) %}
  {{ return(dbt.materialization_table_default()) }}
{% endmaterialization %}
