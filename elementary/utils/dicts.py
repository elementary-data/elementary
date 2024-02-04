from typing import Dict, List


# Flatten a nested dict by a given key, for example:
# nested_dict = {"top1": "one", "nested": {"top1": "One", "top2": "Two"}}, flatten_by_key = "nested" -> {"top1": "One", "top2": "Two"}
def flatten_dict_by_key(nested_dict: Dict, flatten_by_key: str) -> Dict:
    flatten_dict = {**nested_dict, **nested_dict.get(flatten_by_key, {})}
    flatten_dict.pop(flatten_by_key, None)
    return flatten_dict


# Merge dicts attributes into a single list that contains all the values.
def merge_dicts_attribute(dicts: List[Dict], attribute_key: str) -> List:
    merged_attribute = []
    for dict_to_merge in dicts:
        dict_attribute = dict_to_merge.get(attribute_key, [])
        if isinstance(dict_attribute, list):
            valid_items = [item for item in dict_attribute if item is not None]
            merged_attribute.extend(valid_items)
        else:
            if dict_attribute is not None:
                merged_attribute.append(dict_attribute)
    return merged_attribute
