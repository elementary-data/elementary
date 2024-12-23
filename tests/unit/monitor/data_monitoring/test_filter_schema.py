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
