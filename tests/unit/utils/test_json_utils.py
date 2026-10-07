import pytest

from elementary.utils.json_utils import (
    unpack_and_flatten_and_dedup_list_of_strings,
    unpack_and_flatten_str_to_list,
)
from elementary.utils.schema import ExtendedBaseModel


@pytest.mark.parametrize("token", ["12345", "1.5", "true", "false"])
def test_unpack_and_flatten_keeps_json_scalar_tokens(token):
    assert unpack_and_flatten_str_to_list(token) == [token]
    assert unpack_and_flatten_and_dedup_list_of_strings(token) == [token]


def test_unpack_and_flatten_still_handles_lists_and_commas():
    assert unpack_and_flatten_str_to_list('["a", "b"]') == ["a", "b"]
    assert unpack_and_flatten_str_to_list("a, b") == ["a", "b"]
    assert unpack_and_flatten_str_to_list("{}") == []


@pytest.mark.parametrize("token", ["12345", "1.5", "true"])
def test_load_var_to_list_keeps_json_scalar_tokens(token):
    assert ExtendedBaseModel._load_var_to_list(token) == [token]


def test_load_var_to_list_still_handles_lists_and_strings():
    assert ExtendedBaseModel._load_var_to_list('["a"]') == ["a"]
    assert ExtendedBaseModel._load_var_to_list("prod") == ["prod"]


@pytest.mark.parametrize(
    "token, expected",
    [
        ('"@alice"', ["@alice"]),
        ('"prod"', ["prod"]),
        ('" prod "', ["prod"]),
        (" prod ", ["prod"]),
        (" 12345 ", ["12345"]),
        (" true ", ["true"]),
    ],
)
def test_scalar_tokens_match_in_report_and_alerts(token, expected):
    assert ExtendedBaseModel._load_var_to_list(token) == expected
    assert unpack_and_flatten_and_dedup_list_of_strings(token) == expected


@pytest.mark.parametrize("value", ["[12345]", '[1, "a"]', [1, "a"]])
def test_alert_numeric_list_items_are_strings(value):
    from elementary.utils.strings import prettify_and_dedup_list

    result = unpack_and_flatten_and_dedup_list_of_strings(value)
    expected = ["12345"] if value == "[12345]" else ["1", "a"]
    assert sorted(result) == expected
    assert prettify_and_dedup_list(result) == ", ".join(expected)


def test_general_list_parser_preserves_sample_rows():
    rows = [{"a": 1}, {"b": True}]
    assert unpack_and_flatten_str_to_list('[{"a": 1}, {"b": true}]') == rows
    assert ExtendedBaseModel._load_var_to_list(rows) is rows


def test_existing_dict_and_comma_policies_are_preserved():
    assert unpack_and_flatten_and_dedup_list_of_strings('{"a": 1}') == []
    assert ExtendedBaseModel._load_var_to_list('{"a": 1}') == ['{"a": 1}']
    assert sorted(unpack_and_flatten_and_dedup_list_of_strings("a, b")) == ["a", "b"]
    assert ExtendedBaseModel._load_var_to_list("a, b") == ["a, b"]
    assert ExtendedBaseModel._load_var_to_list(["a", "a", "b"]) == ["a", "a", "b"]


@pytest.mark.parametrize("value", ["[12345]", '[1, "a"]', [1, "a"]])
def test_report_owner_lists_match_alert_strings(value):
    from elementary.monitor.fetchers.models.schema import ArtifactSchema

    expected = ["12345"] if value == "[12345]" else ["1", "a"]
    assert ArtifactSchema(owners=value).owners == expected
    assert sorted(unpack_and_flatten_and_dedup_list_of_strings(value)) == expected


def test_dependency_nodes_keep_commas_order_and_duplicates():
    from elementary.monitor.fetchers.models.schema import ExposureSchema

    assert ExposureSchema(depends_on_nodes="a, b").depends_on_nodes == ["a, b"]
    assert ExposureSchema(depends_on_nodes=["a", "a", "b"]).depends_on_nodes == [
        "a",
        "a",
        "b",
    ]
