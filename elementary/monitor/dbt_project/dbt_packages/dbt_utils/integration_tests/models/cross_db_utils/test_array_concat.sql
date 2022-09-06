with data as (

    select 
        data_array_concat.result_as_string,
        array_1.num_input_1 as array_1_num_input_1,
        array_1.num_input_2 as array_1_num_input_2,
        array_1.num_input_3 as array_1_num_input_3,
        array_2.num_input_1 as array_2_num_input_1,
        array_2.num_input_2 as array_2_num_input_2,
        array_2.num_input_3 as array_2_num_input_3
    from {{ ref('data_array_concat') }} as data_array_concat
    left join {{ ref('data_array_construct') }} as array_1
    on data_array_concat.array_1_as_string = array_1.result_as_string
    left join {{ ref('data_array_construct') }} as array_2
    on data_array_concat.array_2_as_string = array_2.result_as_string

),

concat_array as (

    select
        {{ dbt_utils.array_concat(
            dbt_utils.array_construct(['array_1_num_input_1', 'array_1_num_input_2', 'array_1_num_input_3']), 
            dbt_utils.array_construct(['array_2_num_input_1', 'array_2_num_input_2', 'array_2_num_input_3'])
        ) }} as array_actual,
        result_as_string as expected
    from data

)

-- we need to cast the arrays to strings in order to compare them to the output in our seed file  
select
    array_actual,
    {{ dbt_utils.cast_array_to_string('array_actual') }} as actual,
    expected
from concat_array