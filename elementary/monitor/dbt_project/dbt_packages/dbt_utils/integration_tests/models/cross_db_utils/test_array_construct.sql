with data as (

    select * from {{ ref('data_array_construct') }}

),

array_construct as (
    select
        {{ dbt_utils.array_construct(['num_input_1', 'num_input_2', 'num_input_3']) }} as array_actual,
        result_as_string as expected

    from data

    union all

    select
        {{ dbt_utils.array_construct() }} as array_actual,
        '[]' as expected

)

-- we need to cast the arrays to strings in order to compare them to the output in our seed file  
select
    array_actual,
    {{ dbt_utils.cast_array_to_string('array_actual') }} as actual,
    expected
from array_construct