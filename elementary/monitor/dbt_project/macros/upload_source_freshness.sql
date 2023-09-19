{% macro upload_source_freshness(results) %}
  {% set source_freshness_results = fromjson(results) %}
  {% set source_freshness_results_relation = ref('dbt_source_freshness_results') %}
  {% do elementary.upload_artifacts_to_table(source_freshness_results_relation, source_freshness_results, elementary.flatten_source_freshness, append=True, should_commit=true) %}
{% endmacro %}
