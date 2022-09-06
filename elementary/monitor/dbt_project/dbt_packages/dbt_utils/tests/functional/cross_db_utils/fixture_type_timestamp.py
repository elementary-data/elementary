

# type_timestamp

# TODO - implement expected results here
seeds__data_type_timestamp_csv = """todo,result
TODO,1
"""


models__test_type_timestamp_sql = """
with data as (

    select * from {{ ref('data_type_timestamp') }}

)

-- TODO - implement actual logic here
select

    1 as actual,
    1 as expected

from data
"""


models__test_type_timestamp_yml = """
version: 2
models:
  - name: test_type_timestamp
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
