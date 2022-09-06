select 
    id, 
    favorite_number
from 
    {{ ref('test_union_where_base') }}
