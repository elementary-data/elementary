import json
import math
from typing import Any, Dict, List, Optional, Union

from tabulate import tabulate


def try_load_json(value: Optional[Union[str, dict, list]]):
    if value is None:
        return None
    if isinstance(value, dict) or isinstance(value, list):
        # Already loaded
        return value

    try:
        return json.loads(value)
    except Exception:
        return None


def unpack_and_flatten_str_to_list(list_as_str: str) -> List[str]:
    """
    if given a simple token like "marketing" -> return ["marketing"]
    if given a comma delimited token like "marketing, finance" -> return ["marketing", "finance"]
    if given a json of a list -> return that list '["marketing", "finance"]' -> ['marketing', 'finance']
    :param list_as_str:
    :return:
    """
    # we're either dealing with a json of a list, a comma delimited string, or just one string
    list_unpacked = try_load_json(list_as_str)
    if (
        list_unpacked is None
    ):  # it was not a json, could be a comma delimited string, or a simple string.
        return [x.strip() for x in list_as_str.split(",")]

    if isinstance(list_unpacked, list):
        return list_unpacked
    return []  # edge case of a string of an empty dict or IDK


def sum_lists(list_of_lists: List[List]) -> List:
    ret = []
    for list_ in list_of_lists:
        ret.extend(list_)
    return ret


def unpack_and_flatten_and_dedup_list_of_strings(
    list_maybe_jsoned: Optional[Union[List[str], str]]
) -> List[str]:
    if not list_maybe_jsoned:
        return []
    ret = []
    if isinstance(list_maybe_jsoned, str):
        ret = unpack_and_flatten_str_to_list(list_maybe_jsoned)
    elif isinstance(list_maybe_jsoned, list):
        ret = sum_lists(
            [
                unpack_and_flatten_str_to_list(x)
                for x in list_maybe_jsoned
                if isinstance(x, str)
            ]
        )
    return list(set(ret))


def list_of_lists_of_strings_to_comma_delimited_unique_strings(
    list_of_strings: List[List[str]], prefix: Optional[str] = None
) -> str:
    list_of_strings = [x for x in list_of_strings if x]  # filter Nones and empty lists
    flat_list = sum_lists(list_of_strings)
    if prefix:
        flat_list = [append_prefix_if_missing(x, prefix) for x in flat_list]
    unique_strings = list(set(flat_list))
    return ", ".join(unique_strings)


def append_prefix_if_missing(string: str, prefix: str) -> str:
    if string.startswith(prefix):
        return string
    return f"{prefix}{string}"


def inf_and_nan_to_str(obj) -> Any:
    """Replaces occurrences of float("nan") for float("infinity") in the given dict object."""
    if isinstance(obj, float):
        if math.isinf(obj):
            return "Infinity" if obj > 0 else "-Infinity"
        elif math.isnan(obj):
            return "NaN"
        else:
            return obj
    elif isinstance(obj, dict):
        return {k: inf_and_nan_to_str(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [inf_and_nan_to_str(i) for i in obj]
    else:
        return obj


def _format_value(value: Any) -> str:
    """Format a value for table display, avoiding scientific notation for floats."""
    if value is None:
        return ""
    if isinstance(value, float):
        if math.isinf(value) or math.isnan(value):
            return str(value)
        # Format floats without scientific notation
        if value == int(value) and abs(value) < 1e15:
            return str(int(value))
        return f"{value:.10f}".rstrip("0").rstrip(".")
    return str(value)


def list_of_dicts_to_markdown_table(
    data: List[Dict[str, Any]], max_length: Optional[int] = None
) -> str:
    """
    Convert a list of dictionaries with consistent keys to a markdown table string.

    Args:
        data: List of dictionaries
        max_length: Optional maximum character length for the output. If the full
            table exceeds this limit, rows are removed from the end and a
            "(truncated)" note is appended to avoid cutting mid-row.

    Returns:
        A markdown-formatted table string using GitHub table format
    """
    if not data:
        return ""

    processed_data = [{k: _format_value(v) for k, v in row.items()} for row in data]
    full_table = tabulate(
        processed_data, headers="keys", tablefmt="github", disable_numparse=True
    )

    if max_length is None or len(full_table) <= max_length:
        return full_table

    # Truncate by removing rows from the end until the table fits
    truncation_note = "\n(truncated)"
    effective_max = max_length - len(truncation_note)
    for row_count in range(len(processed_data) - 1, 0, -1):
        table = tabulate(
            processed_data[:row_count],
            headers="keys",
            tablefmt="github",
            disable_numparse=True,
        )
        if len(table) <= effective_max:
            return table + truncation_note

    # Even a single row doesn't fit — return what we can with a note
    single_row_table = tabulate(
        processed_data[:1],
        headers="keys",
        tablefmt="github",
        disable_numparse=True,
    )
    return single_row_table + truncation_note
