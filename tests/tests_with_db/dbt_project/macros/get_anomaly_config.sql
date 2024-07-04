{% macro get_anomaly_config(model_config, config) %}
  {% set mock_model = {
    "alias": "mock_model",
    "config": {
      "elementary": model_config
    }
  } %}
  {# trick elementary into thinking this is the running model #}
  {% do context.update({
    "model": {
      "depends_on": {
        "nodes": ["id"]
      }
    },
    "graph": {
      "nodes": {
        "id": mock_model
      }
    }
  }) %}
  {% do return(elementary.get_anomalies_test_configuration(api.Relation.create("db", "schema", "mock_model"), **config)[0]) %}
{% endmacro %}