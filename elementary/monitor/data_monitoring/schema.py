from datetime import datetime
from enum import Enum
from typing import Any, List, Optional

from pydantic import BaseModel, Field, validator

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


class SupportedFilterTypes(Enum):
    IS = "is"


class FilterSchema(BaseModel):
    # The relation between values is OR.
    values: list[Any]
    type: SupportedFilterTypes = SupportedFilterTypes.IS

    class Config:
        # Make sure that serializing Enum return values
        use_enum_values = True


class StatusFilterSchema(FilterSchema):
    values: list[Status]


class ResourceTypeFilterSchema(FilterSchema):
    values: list[ResourceType]


class FiltersSchema(BaseModel):
    selector: Optional[str] = None
    invocation_id: Optional[str] = None
    invocation_time: Optional[str] = Field(default=None)
    last_invocation: Optional[bool] = False
    node_names: Optional[List[str]] = None

    tags: list[FilterSchema] = Field(default_factory=list)
    owners: list[FilterSchema] = Field(default_factory=list)
    models: list[FilterSchema] = Field(default_factory=list)
    statuses: list[StatusFilterSchema] = Field(
        default=[
            StatusFilterSchema(
                type=SupportedFilterTypes.IS,
                values=[Status.FAIL, Status.ERROR, Status.RUNTIME_ERROR, Status.WARN],
            )
        ]
    )
    resource_types: list[ResourceTypeFilterSchema] = Field(default_factory=list)

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

    def to_selector_filter_schema(self) -> "SelectorFilterSchema":
        selector = self.selector if self.selector else None
        invocation_id = self.invocation_id if self.invocation_id else None
        invocation_time = self.invocation_time if self.invocation_time else None
        last_invocation = self.last_invocation if self.last_invocation else False
        node_names = self.node_names if self.node_names else None
        tags = self.tags[0].values[0] if self.tags else None
        owners = self.owners[0].values[0] if self.owners else None
        models = self.models[0].values[0] if self.models else None
        statuses = self.statuses[0].values if self.statuses else None
        resource_types = self.resource_types[0].values if self.resource_types else None

        return SelectorFilterSchema(
            selector=selector,
            invocation_id=invocation_id,
            invocation_time=invocation_time,
            last_invocation=last_invocation,
            node_names=node_names,
            tag=tags,
            owner=owners,
            model=models,
            statuses=statuses,
            resource_types=resource_types,
        )


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


class WarehouseInfo(BaseModel):
    id: str
    type: str
