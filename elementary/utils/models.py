from typing import Optional, Union, List

from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.source_freshness import SourceFreshnessAlert
from elementary.monitor.alerts.test import TestAlert
from elementary.utils.json_utils import try_load_json


def get_shortened_model_name(model):
    if model is None:
        # this can happen for example when a Singular test is failing for having no refs.
        return None
    return model.split(".")[-1]


def alert_to_concise_name(alert):
    if isinstance(alert, TestAlert):
        return f"{alert.test_short_name or alert.test_name} - {alert.test_sub_type_display_name}"
    elif isinstance(alert, SourceFreshnessAlert):
        return f"source freshness alert - {alert.source_name}.{alert.identifier}"
    elif isinstance(alert, ModelAlert):
        if alert.materialization == "snapshot":
            text = "snapshot"
        else:
            text = "model"
        return f"dbt {text} alert - {alert.alias}"
    return "Alert"  # used only in Unit Tests


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
    if list_unpacked is None:  # it was not a json, could be a comma delimited string, or a simple string.
        return [x.strip() for x in list_as_str.split(",")]

    if isinstance(list_unpacked, list):
        return list_unpacked
    return []  # edge case of a string of an empty dict or IDK


def unpack_and_flatten_and_dedup_list_of_strings(list_maybe_jsoned: Optional[Union[List[str], str]]) -> List[str]:
    if not list_maybe_jsoned:
        return []
    ret = []
    if isinstance(list_maybe_jsoned, str):
        ret = unpack_and_flatten_str_to_list(list_maybe_jsoned)
    elif isinstance(list_maybe_jsoned, list):
        ret = [unpack_and_flatten_str_to_list(x) for x in list_maybe_jsoned]
        ret = sum(ret, start=[])
    return list(set(ret))

def list_of_strings_to_comma_delimited_unique_strings(list_of_strings: List[List[str]], prefix: str=None) -> str:
    flat_list = sum(list_of_strings, start=[])
    if prefix:
        flat_list = [append_prefix_if_missing(x, prefix) for x in flat_list]
    unique_strings = list(set(flat_list))
    return ", ".join(unique_strings)

def append_prefix_if_missing(string: str, prefix: str) -> str:
    if string.startswith(prefix):
        return string
    return f"{prefix}{string}"


