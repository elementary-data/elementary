from typing import List, Union

from elementary.utils.json_utils import unpack_and_flatten_str_to_list


def pluralize_string(number, singular_form, plural_form):
    if number == 1:
        return f"{number} {singular_form}"
    else:
        return f"{number} {plural_form}"


def prettify_and_dedup_list(str_list: Union[List[str], str]) -> str:
    """
    Receives a list of strings, either JSON dumped or not, dedups and sorts it, and returns it as a comma-separated
    string.
    This is useful for various lists we include in Slack messages (owners, subscribers, etc.)
    """
    if isinstance(str_list, str):
        str_list = unpack_and_flatten_str_to_list(str_list)
    return ", ".join(sorted(set(str_list)))
