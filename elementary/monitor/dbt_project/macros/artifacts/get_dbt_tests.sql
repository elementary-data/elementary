{%- macro get_dbt_tests(
    database_name=none,
    schema_name=none,
    name=none,
    package_name=none,
    test_type=none,
    test_namespace=none,
    severity=none,
    parent_model_unique_id=none,
    quality_dimension=none,
    group_name=none,
    generated_after=none,
    generated_before=none,
    limit=200
) -%}
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
            type,
            test_namespace,
            test_original_name,
            parent_model_unique_id,
            package_name,
            quality_dimension,
            group_name,
            tags,
            model_tags,
            model_owners,
            description,
            path,
            generated_at
        from {{ relation }}
        where 1=1
        {{ elementary_cli._eq_ci_filter('database_name', database_name) }}
        {{ elementary_cli._eq_ci_filter('schema_name', schema_name) }}
        {%- if name is not none %}
        and (
            lower(name) like lower('%{{ elementary_cli._sql_escape(name) }}%')
            or lower(short_name) like lower('%{{ elementary_cli._sql_escape(name) }}%')
            or lower(alias) like lower('%{{ elementary_cli._sql_escape(name) }}%')
        )
        {%- endif %}
        {{ elementary_cli._eq_ci_filter('package_name', package_name) }}
        {{ elementary_cli._eq_ci_filter('type', test_type) }}
        {{ elementary_cli._eq_ci_filter('test_namespace', test_namespace) }}
        {{ elementary_cli._eq_ci_filter('severity', severity) }}
        {{ elementary_cli._eq_filter('parent_model_unique_id', parent_model_unique_id) }}
        {{ elementary_cli._eq_ci_filter('quality_dimension', quality_dimension) }}
        {{ elementary_cli._eq_ci_filter('group_name', group_name) }}
        {{ elementary_cli._gte_filter('generated_at', generated_after) }}
        {{ elementary_cli._lte_filter('generated_at', generated_before) }}
        order by unique_id
        limit {{ limit }}
    {% endset %}

    {% set results = elementary.run_query(query) %}
    {% do return(elementary.agate_to_dicts(results)) %}
{%- endmacro -%}
