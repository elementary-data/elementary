{% set exclude_field = 'FIELD_3' %}


with data as (

    select
        {{ dbt_utils.star(from=ref('data_star'), except=[exclude_field]) }}

    from {{ ref('data_star') }}

)

select * from data
