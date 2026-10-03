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
