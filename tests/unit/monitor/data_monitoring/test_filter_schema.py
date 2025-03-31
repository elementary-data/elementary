import pytest

from elementary.monitor.data_monitoring.schema import FilterSchema, FilterType


def test_filter_schema_is_operator():
    filter_schema = FilterSchema(values=["test1", "test2"], type=FilterType.IS)

    # Should match when value is in the filter values
    assert filter_schema.apply_filter_on_value("test1") is True
    assert filter_schema.apply_filter_on_value("test2") is True

    # Should not match when value is not in filter values
    assert filter_schema.apply_filter_on_value("test3") is False


def test_filter_schema_is_not_operator():
    filter_schema = FilterSchema(values=["test1", "test2"], type=FilterType.IS_NOT)

    # Should not match when value is in the filter values
    assert filter_schema.apply_filter_on_value("test1") is False
    assert filter_schema.apply_filter_on_value("test2") is False

    # Should match when value is not in filter values
    assert filter_schema.apply_filter_on_value("test3") is True


def test_filter_schema_apply_filter_on_values_is_operator():
    filter_schema = FilterSchema(values=["test1", "test2"], type=FilterType.IS)

    # Should match when any value matches (ANY_OPERATORS)
    assert filter_schema.apply_filter_on_values(["test1", "test3"]) is True
    assert filter_schema.apply_filter_on_values(["test3", "test4"]) is False


def test_filter_schema_apply_filter_on_values_is_not_operator():
    filter_schema = FilterSchema(values=["test1", "test2"], type=FilterType.IS_NOT)

    # Should match all values for IS_NOT (ALL_OPERATORS)
    assert filter_schema.apply_filter_on_values(["test3", "test4"]) is True
    assert filter_schema.apply_filter_on_values(["test1", "test3"]) is False


def test_filter_schema_invalid_filter_type():
    with pytest.raises(ValueError):
        FilterSchema(values=["test1"], type="invalid")  # type: ignore[arg-type]


def test_filter_schema_contains_operator():
    filter_schema = FilterSchema(values=["test"], type=FilterType.CONTAINS)

    # Should match when value contains the filter value
    assert filter_schema.apply_filter_on_value("test123") is True
    assert filter_schema.apply_filter_on_value("123test") is True
    assert filter_schema.apply_filter_on_value("123test456") is True

    # Should match case-insensitive
    assert filter_schema.apply_filter_on_value("TEST123") is True
    assert filter_schema.apply_filter_on_value("123TEST") is True

    # Should not match when value doesn't contain filter value
    assert filter_schema.apply_filter_on_value("123") is False


def test_filter_schema_apply_filter_on_values_contains_operator():
    filter_schema = FilterSchema(values=["test1", "test2"], type=FilterType.CONTAINS)

    # Should match when any value contains any filter value
    assert filter_schema.apply_filter_on_values(["abc_test1_def", "xyz"]) is True
    assert filter_schema.apply_filter_on_values(["abc", "xyz_test2"]) is True

    # Should not match when no values contain any filter values
    assert filter_schema.apply_filter_on_values(["abc", "xyz"]) is False


def test_get_matching_values() -> None:
    filter_schema = FilterSchema(values=["test1", "test2"], type=FilterType.IS)
    values = ["test1", "test3", "test4"]
    assert filter_schema.get_matching_values(values) == {"test1"}

    filter_schema = FilterSchema(values=["test"], type=FilterType.CONTAINS)
    values = ["test1", "testing", "other"]
    assert filter_schema.get_matching_values(values) == {"test1", "testing"}

    filter_schema = FilterSchema(values=["test1"], type=FilterType.IS_NOT)
    values = ["test2", "test3"]
    assert filter_schema.get_matching_values(values) == {"test2", "test3"}

    filter_schema = FilterSchema(values=["test1"], type=FilterType.IS_NOT)
    values = ["test1", "test2", "test3"]
    assert filter_schema.get_matching_values(values) == set()

    filter_schema = FilterSchema(values=["test"], type=FilterType.IS)
    filter_schema.type = "unsupported"  # type: ignore
    with pytest.raises(ValueError, match="Unsupported filter type: unsupported"):
        filter_schema.get_matching_values(values)
