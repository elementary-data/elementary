{%- macro get_dbt_source(unique_id) -%}
    {% set relation = ref('elementary', 'dbt_sources') %}
    {% if not elementary.relation_exists(relation) %}
        {% do return([]) %}
    {% endif %}

    {% set query %}
        select
            unique_id,
            name,
            database_name,
            schema_name,
            source_name,
            identifier,
            description,
            package_name,
            path,
            original_path,
            freshness_warn_after,
            freshness_error_after,
            freshness_description,
            loaded_at_field,
            source_description,
            tags,
            owner,
            generated_at
        from {{ relation }}
        where unique_id = '{{ elementary_cli._sql_escape(unique_id) }}'
        limit 1
    {% endset %}

    {% set results = elementary.run_query(query) %}
    {% do return(elementary.agate_to_dicts(results)) %}
{%- endmacro -%}
