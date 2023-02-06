import json
from typing import Union


def try_load_json(string_value: str):
    try:
        return json.loads(string_value)
    except Exception:
        return None


def prettify_json_str_set(str_json_list: Union[str, list]) -> str:
    if not str_json_list:
        return ""

    json_obj = try_load_json(str_json_list)
    if isinstance(json_obj, list):
        return ", ".join(set(json_obj))
    return str_json_list
