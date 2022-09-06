with some_model as (
    select 1 as id, 'abc' as key_name, 'dbt' as static_col union all 
    select 2 as id, 'abc' as key_name, 'dbt' as static_col union all
    select 3 as id, 'jkl' as key_name, 'dbt' as static_col union all
    select 4 as id, 'jkl' as key_name, 'dbt' as static_col union all
    select 5 as id, 'jkl' as key_name, 'dbt' as static_col union all
    select 6 as id, 'xyz' as key_name, 'test' as static_col
),

final as (
    select 
        key_name, 
        {{ dbt_utils.any_value('static_col') }} as static_col, 
        count(id) as num_rows
    from some_model
    group by key_name
)

select * from final