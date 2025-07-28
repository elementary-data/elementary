{% materialization test, default %}
  {% if var('enable_elementary_test_materialization', false) %}
    {% do return(elementary.materialization_test_default()) %}
  {% else %}
    {% do return(dbt.materialization_test_default()) %}
  {% endif %}
{% endmaterialization %}

{% materialization test, adapter="snowflake" %}
  {% if var('enable_elementary_test_materialization', false) %}
    {% do return(elementary.materialization_test_snowflake()) %}
  {% else %}
    {% if dbt.materialization_test_snowflake %}
      {% do return(dbt.materialization_test_snowflake()) %}
    {% else %}
      {% do return(dbt.materialization_test_default()) %}
    {% endif %}
  {% endif %}
{% endmaterialization %}
