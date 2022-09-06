
# dateadd

seeds__data_dateadd_csv = """from_time,interval_length,datepart,result
2018-01-01 01:00:00,1,day,2018-01-02 01:00:00
2018-01-01 01:00:00,1,month,2018-02-01 01:00:00
2018-01-01 01:00:00,1,year,2019-01-01 01:00:00
2018-01-01 01:00:00,1,hour,2018-01-01 02:00:00
,1,day,
"""


models__test_dateadd_sql = """
with data as (

    select * from {{ ref('data_dateadd') }}

)

select
    case
        when datepart = 'hour' then cast({{ dbt_utils.dateadd('hour', 'interval_length', 'from_time') }} as {{dbt_utils.type_timestamp()}})
        when datepart = 'day' then cast({{ dbt_utils.dateadd('day', 'interval_length', 'from_time') }} as {{dbt_utils.type_timestamp()}})
        when datepart = 'month' then cast({{ dbt_utils.dateadd('month', 'interval_length', 'from_time') }} as {{dbt_utils.type_timestamp()}})
        when datepart = 'year' then cast({{ dbt_utils.dateadd('year', 'interval_length', 'from_time') }} as {{dbt_utils.type_timestamp()}})
        else null
    end as actual,
    result as expected

from data
"""

models__test_dateadd_yml = """
version: 2
models:
  - name: test_dateadd
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
