{% macro get_exposures() %}
    {% set dbt_exposures_relation = ref('elementary', 'dbt_exposures') %}
    {% set label_column_exists = elementary.column_exists_in_relation(dbt_exposures_relation, 'label') %}
    {%- if elementary.relation_exists(dbt_exposures_relation) -%}
        --{# TODO: should we group by #}
        {% set get_exposures_query %}
              with dbt_artifacts_exposures as (
                select
                  name,
                  {% if label_column_exists %}
                    label,
                  {% endif %}
                  unique_id,
                  url,
                  type,
                  maturity,
                  owner_email,
                  owner_name as owners,
                  tags,
                  package_name,
                  description,
                  meta,
                  original_path as full_path
                from {{ dbt_exposures_relation }}
              )

             select * from dbt_artifacts_exposures
        {% endset %}
        {% set exposures_agate = run_query(get_exposures_query) %}
        {% do return(elementary.agate_to_dicts(exposures_agate)) %}
    {%- endif -%}
{% endmacro %}
