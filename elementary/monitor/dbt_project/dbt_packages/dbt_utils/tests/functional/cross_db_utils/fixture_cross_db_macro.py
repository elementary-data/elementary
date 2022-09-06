macros__test_assert_equal_sql = """
{% test assert_equal(model, actual, expected) %}
select * from {{ model }} where {{ actual }} != {{ expected }}

{% endtest %}
"""
