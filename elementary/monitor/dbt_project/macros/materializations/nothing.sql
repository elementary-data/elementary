{# Materialization that does not create any table at the end of it runs #}
{# It is used when we can't update tables using run-operation so we workaround and run a model that updates an other table for example #}
{% materialization nothing, default -%}
  {# The main statement execute the model, but does not create any table / view on the DWH #}
  {% call statement('main') -%}
    {{ sql }}
  {%- endcall %}
  {{ adapter.commit() }}
  {{ return({'relations': []}) }}
{% endmaterialization %}
