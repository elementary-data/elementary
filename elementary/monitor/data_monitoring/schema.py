from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, validator

from elementary.utils.log import get_logger
from elementary.utils.time import DATETIME_FORMAT, convert_local_time_to_timezone

logger = get_logger(__name__)


class InvalidSelectorError(Exception):
    pass


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

    def validate_report_selector(self):
        # If we start supporting multiple selectors we need to change this logic
        if not self.selector:
            return

        valid_report_selectors = ["last_invocation", "invocation_id", "invocation_time"]
        if all(
            [
                selector_type not in self.selector
                for selector_type in valid_report_selectors
            ]
        ):
            raise InvalidSelectorError(
                "Selector is invalid for report: ", self.selector
            )

    def validate_alert_selector(self):
        # If we start supporting multiple selectors we need to change this logic
        if not self.selector:
            return

        invalid_alert_selectors = [
            "last_invocation",
            "invocation_id",
            "invocation_time",
        ]
        if any(
            [
                selector_type in self.selector
                for selector_type in invalid_alert_selectors
            ]
        ):
            raise InvalidSelectorError(
                "Selector is invalid for alerts: ", self.selector
            )


class WarehouseInfo(BaseModel):
    id: str
    type: str
