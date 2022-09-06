/*This test checks that column aliases aren't applied unless there's a prefix/suffix necessary, to ensure that GROUP BYs keep working*/

{% set selected_columns = dbt_utils.star(from=ref('data_star_aggregate'), except=['value_field']) %}

with data as (

    select
        {{ selected_columns }},
        sum(value_field) as value_field_sum

    from {{ ref('data_star_aggregate') }}
    group by {{ selected_columns }}

)

select * from data
