import json


def try_load_json(string_value: str):
    try:
        return json.loads(string_value)
    except Exception:
        return None