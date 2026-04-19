{%- macro get_dbt_sources(
    database_name=none,
    schema_name=none,
    source_name=none,
    name=none,
    identifier=none,
    package_name=none,
    generated_after=none,
    generated_before=none,
    limit=200
) -%}
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
        where 1=1
        {{ elementary_cli._eq_ci_filter('database_name', database_name) }}
        {{ elementary_cli._eq_ci_filter('schema_name', schema_name) }}
        {{ elementary_cli._eq_ci_filter('source_name', source_name) }}
        {{ elementary_cli._like_ci_filter('name', name) }}
        {{ elementary_cli._eq_ci_filter('identifier', identifier) }}
        {{ elementary_cli._eq_ci_filter('package_name', package_name) }}
        {{ elementary_cli._gte_filter('generated_at', generated_after) }}
        {{ elementary_cli._lte_filter('generated_at', generated_before) }}
        order by unique_id
        limit {{ limit }}
    {% endset %}

    {% set results = elementary.run_query(query) %}
    {% do return(elementary.agate_to_dicts(results)) %}
{%- endmacro -%}
