import re
from datetime import datetime
from enum import Enum
from typing import Any, List, Optional, Pattern, Tuple

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
    values: List[Any]
    type: SupportedFilterTypes = SupportedFilterTypes.IS

    class Config:
        # Make sure that serializing Enum return values
        use_enum_values = True


class StatusFilterSchema(FilterSchema):
    values: List[Status]


class ResourceTypeFilterSchema(FilterSchema):
    values: List[ResourceType]


def _get_default_statuses_filter() -> List[StatusFilterSchema]:
    return [
        StatusFilterSchema(
            type=SupportedFilterTypes.IS,
            values=[Status.FAIL, Status.ERROR, Status.RUNTIME_ERROR, Status.WARN],
        )
    ]


class FiltersSchema(BaseModel):
    selector: Optional[str] = None
    invocation_id: Optional[str] = None
    invocation_time: Optional[str] = None
    last_invocation: Optional[bool] = False
    node_names: List[str] = Field(default_factory=list)

    tags: List[FilterSchema] = Field(default_factory=list)
    owners: List[FilterSchema] = Field(default_factory=list)
    models: List[FilterSchema] = Field(default_factory=list)
    statuses: List[StatusFilterSchema] = Field(default=_get_default_statuses_filter())
    resource_types: List[ResourceTypeFilterSchema] = Field(default_factory=list)

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

    @staticmethod
    def from_cli_params(cli_filters: Tuple[str]) -> "FiltersSchema":
        if not cli_filters:
            return FiltersSchema()

        tags = []
        owners = []
        models = []
        statuses = []
        resource_types = []

        for cli_filter in cli_filters:
            tags_match = FiltersSchema._match_filter_regex(
                filter_string=cli_filter, regex=re.compile(r"tags:(.*)")
            )
            if tags_match:
                tags.append(FilterSchema(values=tags_match))
                continue

            owners_match = FiltersSchema._match_filter_regex(
                filter_string=cli_filter, regex=re.compile(r"owners:(.*)")
            )
            if owners_match:
                owners.append(FilterSchema(values=owners_match))
                continue

            models_match = FiltersSchema._match_filter_regex(
                filter_string=cli_filter, regex=re.compile(r"models:(.*)")
            )
            if models_match:
                models.append(FilterSchema(values=models_match))
                continue

            statuses_match = FiltersSchema._match_filter_regex(
                filter_string=cli_filter, regex=re.compile(r"statuses:(.*)")
            )
            if statuses_match:
                statuses.append(
                    StatusFilterSchema(
                        values=[Status(status) for status in statuses_match]
                    )
                )
                continue

            resource_types_match = FiltersSchema._match_filter_regex(
                filter_string=cli_filter, regex=re.compile(r"resource_types:(.*)")
            )
            if resource_types_match:
                resource_types.append(
                    ResourceTypeFilterSchema(
                        values=[
                            ResourceType(resource_type)
                            for resource_type in resource_types_match
                        ]
                    )
                )
                continue

            logger.warning(
                f'Filter "{cli_filter.split(":")[0]}" is not supported - Skipping this filter ("{cli_filter}").'
            )

        return FiltersSchema(
            tags=tags,
            owners=owners,
            models=models,
            statuses=statuses if statuses else _get_default_statuses_filter(),
            resource_types=resource_types,
        )

    @staticmethod
    def _match_filter_regex(filter_string: str, regex: Pattern) -> List[str]:
        match = regex.search(filter_string)
        if match:
            return match.group(1).split(",")
        return []

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


class WarehouseInfo(BaseModel):
    id: str
    type: str
