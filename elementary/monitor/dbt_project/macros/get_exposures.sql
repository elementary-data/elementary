{% macro get_exposures() %}
    {% set exposures_relation = ref('elementary_cli', 'enriched_exposures') %}
    {% if not elementary.relation_exists(exposures_relation) %}
        {% set exposures_relation = ref('elementary', 'dbt_exposures') %}
    {% endif %}
    {% set label_column_exists = elementary.column_exists_in_relation(exposures_relation, 'label') %}
    {% set raw_queries_column_exists = elementary.column_exists_in_relation(exposures_relation, 'raw_queries') %}
    {% set depends_on_columns_column_exists = elementary.column_exists_in_relation(exposures_relation, 'depends_on_columns') %}
    {%- if elementary.relation_exists(exposures_relation) -%}
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
                  original_path as full_path,
                  {% if raw_queries_column_exists %}
                    raw_queries,
                  {% else %}
                    NULL as raw_queries,
                  {% endif %}
                  depends_on_nodes,
                  {% if depends_on_columns_column_exists %}
                    depends_on_columns
                  {% else %}
                    NULL as depends_on_columns
                  {% endif %}

                from {{ exposures_relation }}
              )

             select * from dbt_artifacts_exposures
        {% endset %}
        {% set exposures_agate = run_query(get_exposures_query) %}
        {% do return(elementary.agate_to_dicts(exposures_agate)) %}
    {%- endif -%}
{% endmacro %}
