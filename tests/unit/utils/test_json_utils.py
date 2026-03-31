from elementary.utils.json_utils import list_of_dicts_to_markdown_table


def test_list_of_dicts_to_markdown_table_empty():
    assert list_of_dicts_to_markdown_table([]) == ""


def test_list_of_dicts_to_markdown_table_single_row():
    result = list_of_dicts_to_markdown_table([{"a": 1, "b": "two"}])
    # tabulate "github" format pads columns; assert header and row content
    assert "a" in result and "b" in result
    assert "1" in result and "two" in result
    assert "|" in result and "-----" in result


def test_list_of_dicts_to_markdown_table_multiple_rows():
    data = [
        {"col1": "a", "col2": "b"},
        {"col1": "c", "col2": "d"},
    ]
    result = list_of_dicts_to_markdown_table(data)
    assert "col1" in result and "col2" in result
    assert "a" in result and "b" in result and "c" in result and "d" in result
    assert result.count("\n") >= 3  # header, separator, 2 data rows


def test_list_of_dicts_to_markdown_table_none_values():
    result = list_of_dicts_to_markdown_table([{"x": None, "y": "ok"}])
    assert "x" in result and "y" in result
    assert "ok" in result
    # None is formatted as empty string (empty cell between pipes)
    assert "|" in result


def test_list_of_dicts_to_markdown_table_float_int_like():
    """Floats that are whole numbers are formatted as ints (no scientific notation)."""
    result = list_of_dicts_to_markdown_table([{"n": 1.0}, {"n": 2.0}])
    assert "n" in result
    assert " 1 " in result or "| 1 " in result
    assert " 2 " in result or "| 2 " in result


def test_list_of_dicts_to_markdown_table_float_decimal():
    """Decimal floats are formatted without scientific notation."""
    result = list_of_dicts_to_markdown_table([{"x": 1.23456789}])
    assert "1.23456789" in result or "1.2345678" in result


def test_list_of_dicts_to_markdown_table_inf_nan():
    """inf and nan are stringified."""
    data = [
        {"v": float("inf")},
        {"v": float("-inf")},
        {"v": float("nan")},
    ]
    result = list_of_dicts_to_markdown_table(data)
    assert "inf" in result
    assert "nan" in result
