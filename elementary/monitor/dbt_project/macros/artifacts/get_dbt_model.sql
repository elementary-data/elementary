{%- macro get_dbt_model(unique_id) -%}
    {% set relation = ref('elementary', 'dbt_models') %}
    {% if not elementary.relation_exists(relation) %}
        {% do return([]) %}
    {% endif %}

    {% set query %}
        select
            unique_id,
            name,
            database_name,
            schema_name,
            alias,
            description,
            package_name,
            materialization,
            path,
            original_path,
            patch_path,
            depends_on_nodes,
            group_name,
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
