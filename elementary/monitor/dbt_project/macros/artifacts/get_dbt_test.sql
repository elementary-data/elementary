{%- macro get_dbt_test(unique_id) -%}
    {% set relation = ref('elementary', 'dbt_tests') %}
    {% if not elementary.relation_exists(relation) %}
        {% do return([]) %}
    {% endif %}

    {% set query %}
        select
            unique_id,
            name,
            short_name,
            alias,
            database_name,
            schema_name,
            test_column_name,
            severity,
            warn_if,
            error_if,
            test_params,
            test_namespace,
            test_original_name,
            type,
            parent_model_unique_id,
            package_name,
            quality_dimension,
            group_name,
            tags,
            model_tags,
            model_owners,
            meta,
            depends_on_macros,
            depends_on_nodes,
            description,
            original_path,
            path,
            generated_at
        from {{ relation }}
        where unique_id = '{{ elementary_cli._sql_escape(unique_id) }}'
        limit 1
    {% endset %}

    {% set results = elementary.run_query(query) %}
    {% do return(elementary.agate_to_dicts(results)) %}
{%- endmacro -%}
