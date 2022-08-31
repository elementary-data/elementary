{% macro dbt_sources_query() %}
    with all_dbt_sources as (
        select * from {{ ref('elementary', 'dbt_sources') }}
    )

    select *
    from all_dbt_sources
    where package_name != 'elementary'
{% endmacro %}
