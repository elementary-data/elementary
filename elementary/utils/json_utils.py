import json
from typing import List, Optional, Union


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
