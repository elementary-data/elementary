from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, validator

from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.source_freshness import SourceFreshnessAlert
from elementary.monitor.alerts.test import TestAlert
from elementary.utils.log import get_logger
from elementary.utils.time import DATETIME_FORMAT, convert_local_time_to_timezone

logger = get_logger(__name__)


class Status(Enum):
    WARN = "warn"
    FAIL = "fail"
    SKIPPED = "skipped"
    ERROR = "error"
    RUNTIME_ERROR = "runtime error"


class ResourceType(Enum):
    TEST = "test"
    MODEL = "model"
    SOURCE_FRESHNESS = "source_freshness"

    @staticmethod
    def from_table_name(table_name):
        if table_name == TestAlert.TABLE_NAME:
            return ResourceType.TEST
        elif table_name == ModelAlert.TABLE_NAME:
            return ResourceType.MODEL
        elif table_name == SourceFreshnessAlert.TABLE_NAME:
            return ResourceType.SOURCE_FRESHNESS
        else:
            logger.warning(f"Unknown table name: {table_name}")
            return None


class SelectorFilterSchema(BaseModel):
    selector: Optional[str] = None
    invocation_id: Optional[str] = None
    invocation_time: Optional[str] = None
    last_invocation: Optional[bool] = False
    tag: Optional[str] = None
    owner: Optional[str] = None
    model: Optional[str] = None
    statuses: Optional[List[Status]] = [
        Status.FAIL,
        Status.ERROR,
        Status.RUNTIME_ERROR,
        Status.WARN,
    ]
    resource_types: Optional[List[ResourceType]] = None
    node_names: Optional[List[str]] = None

    @validator("invocation_time", pre=True)
    def format_invocation_time(cls, invocation_time):
        if invocation_time:
            try:
                invocation_datetime = convert_local_time_to_timezone(
                    datetime.fromisoformat(invocation_time)
                )
                return invocation_datetime.strftime(DATETIME_FORMAT)
            except ValueError as err:
                logger.error(
                    f"Failed to parse invocation time filter: {err}\nPlease use a valid ISO 8601 format"
                )
                raise
        return None


class WarehouseInfo(BaseModel):
    id: str
    type: str
