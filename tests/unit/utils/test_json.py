import json

from elementary.utils import json_utils


def test_dumps_with_nan_and_infinity():
    dict_1 = dict(a=float("inf"), b=float("nan"))
    dumped_output = json.dumps(
        dict_1, default=json_utils.nan_or_infinity_to_str, sort_keys=True
    )
    assert dumped_output == '{"a": "Infinity", "b": "NaN"}'
