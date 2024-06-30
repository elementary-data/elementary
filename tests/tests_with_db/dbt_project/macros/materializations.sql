{% materialization test, default %}
  {% if var('enable_elementary_test_materialization', false) %}
    {% do return(elementary.materialization_test_default.call_macro()) %}
  {% else %}
    {% do return(dbt.materialization_test_default.call_macro()) %}
  {% endif %}
{% endmaterialization %}

{% materialization test, adapter="snowflake" %}
  {% if var('enable_elementary_test_materialization', false) %}
    {% do return(elementary.materialization_test_snowflake.call_macro()) %}
  {% else %}
    {% if dbt.materialization_test_snowflake %}
      {% do return(dbt.materialization_test_snowflake.call_macro()) %}
    {% else %}
      {% do return(dbt.materialization_test_default.call_macro()) %}
    {% endif %}
  {% endif %}
{% endmaterialization %}
