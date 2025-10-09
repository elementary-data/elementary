{% materialization test, default %}
  {% do return(elementary.materialization_test_default()) %}
{% endmaterialization %}

{% materialization test, adapter="snowflake" %}
  {% do return(elementary.materialization_test_snowflake()) %}
{% endmaterialization %}
