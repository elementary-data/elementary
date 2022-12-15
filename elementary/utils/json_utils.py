import ast
import json
from typing import List, Union


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


def parse_str_to_list(string_value: str) -> List[str]:
    try:
        return ast.literal_eval(string_value)
    except Exception:
        return [part.strip() for part in string_value.split(",")]
