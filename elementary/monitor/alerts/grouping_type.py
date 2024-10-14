from enum import Enum


class GroupingType(str, Enum):
    BY_ALERT = "alert"
    BY_TABLE = "table"
