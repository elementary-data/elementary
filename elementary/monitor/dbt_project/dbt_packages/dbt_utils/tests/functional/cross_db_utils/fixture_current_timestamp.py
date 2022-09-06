
# current_timestamp

# TODO how can we test this better?
models__test_current_timestamp_sql = """
select
    {{ dbt_utils.current_timestamp() }} as actual,
    {{ dbt_utils.current_timestamp() }} as expected
"""


models__test_current_timestamp_yml = """
version: 2
models:
  - name: test_current_timestamp
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
