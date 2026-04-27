{%- macro get_dbt_models(
    database_name=none,
    schema_name=none,
    materialization=none,
    name=none,
    package_name=none,
    group_name=none,
    generated_after=none,
    generated_before=none,
    limit=200
) -%}
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
        where 1=1
        {{ elementary_cli._eq_ci_filter('database_name', database_name) }}
        {{ elementary_cli._eq_ci_filter('schema_name', schema_name) }}
        {{ elementary_cli._eq_ci_filter('materialization', materialization) }}
        {%- if name is not none %}
        and (
            lower(name) like lower('%{{ elementary_cli._sql_escape(name) }}%')
            or lower(alias) like lower('%{{ elementary_cli._sql_escape(name) }}%')
        )
        {%- endif %}
        {{ elementary_cli._eq_ci_filter('package_name', package_name) }}
        {{ elementary_cli._eq_ci_filter('group_name', group_name) }}
        {{ elementary_cli._gte_filter('generated_at', generated_after) }}
        {{ elementary_cli._lte_filter('generated_at', generated_before) }}
        order by unique_id
        limit {{ limit }}
    {% endset %}

    {% set results = elementary.run_query(query) %}
    {% do return(elementary.agate_to_dicts(results)) %}
{%- endmacro -%}
