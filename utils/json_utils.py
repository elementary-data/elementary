import json


def try_load_json(string_value: str):
    if not string_value or not isinstance(string_value, str):
        return None

    try:
        return json.loads(string_value)
    except Exception:
        return None