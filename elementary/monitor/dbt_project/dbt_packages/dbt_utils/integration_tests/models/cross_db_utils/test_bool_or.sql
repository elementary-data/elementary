select 
    key, 
    {{ dbt_utils.bool_or('val1 = val2') }} as value
from {{ ref('data_bool_or' )}}
group by key