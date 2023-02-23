{% macro get_exposures() %}
    {% set dbt_exposures_relation = ref('elementary', 'dbt_exposures') %}
    {%- if elementary.relation_exists(dbt_exposures_relation) -%}
        --{# TODO: should we group by #}
        {% set get_exposures_query %}
              with dbt_artifacts_exposures as (
                select
                  name,
                  unique_id,
                  url,
                  type,
                  maturity,
                  owner_email,
                  owner_name as owners,
                  tags,
                  package_name,
                  description,
                  original_path as full_path
                from {{ dbt_exposures_relation }}
              )

             select * from dbt_artifacts_exposures
        {% endset %}
        {% set exposures_agate = run_query(get_exposures_query) %}
        {% do return(elementary.agate_to_dicts(exposures_agate)) %}
    {%- endif -%}
{% endmacro %}
