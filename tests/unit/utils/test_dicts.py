import json

from elementary.utils.dicts import flatten_dict_by_key, merge_dicts_attribute


def test_flatten_dict_by_key():
    NESTED_DICT = dict(top=2, another_top=2)
    DICT = dict(top=1, nested=NESTED_DICT)

    # Flat by nested key
    flatten_dict = flatten_dict_by_key(DICT, "nested")
    assert flatten_dict.get("top") == 2
    assert flatten_dict.get("another_top") == 2
    assert flatten_dict.get("nested") is None

    # Flat by ney existing key
    flatten_dict = flatten_dict_by_key(DICT, "nope")
    assert flatten_dict.get("top") == 1
    assert flatten_dict.get("another_top") is None
    assert json.dumps(flatten_dict.get("nested"), sort_keys=True) == json.dumps(NESTED_DICT, sort_keys=True)


def test_merge_dicts_attribute():
    dict_1 = dict(attr=[1, 2, 2])
    dict_2 = dict(attr=3)
    dict_3 = dict()
    assert sorted(merge_dicts_attribute(dicts=[dict_1, dict_2, dict_3], attribute_key="attr")) == sorted([1, 2, 2, 3])
