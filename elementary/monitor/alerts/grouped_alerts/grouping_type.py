from enum import Enum


class GroupingType(str, Enum):
    BY_ALERT = "alert"
    BY_TABLE = "table"
    ALL_IN_ONE = "all_in_one"
