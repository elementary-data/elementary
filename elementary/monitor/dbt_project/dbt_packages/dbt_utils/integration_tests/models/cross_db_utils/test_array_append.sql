with data as (

    select 
        data_array_append.element,
        data_array_append.result_as_string,
        data_array_construct.num_input_1,
        data_array_construct.num_input_2,
        data_array_construct.num_input_3
    from {{ ref('data_array_append') }} as data_array_append
    left join {{ ref('data_array_construct') }} as data_array_construct
    on data_array_append.array_as_string = data_array_construct.result_as_string

),

appended_array as (

    select
        {{ dbt_utils.array_append(dbt_utils.array_construct(['num_input_1', 'num_input_2', 'num_input_3']), 'element') }} as array_actual,
        result_as_string as expected
    from data

)

-- we need to cast the arrays to strings in order to compare them to the output in our seed file  
select
    array_actual,
    {{ dbt_utils.cast_array_to_string('array_actual') }} as actual,
    expected
from appended_array