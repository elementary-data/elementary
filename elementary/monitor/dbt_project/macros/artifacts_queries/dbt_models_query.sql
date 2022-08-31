{% macro dbt_models_query() %}
    with all_dbt_models as (
        select * from {{ ref('elementary', 'dbt_models') }}
    )

    select *
    from all_dbt_models
    where package_name != 'elementary'
{% endmacro %}
