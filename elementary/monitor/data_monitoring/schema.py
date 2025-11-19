import re
from datetime import datetime
from enum import Enum
from functools import cached_property
from typing import Any, Generic, Iterable, List, Optional, Pattern, Set, Tuple, TypeVar

from elementary.utils.log import get_logger
from elementary.utils.pydantic_shim import BaseModel, Field, validator
from elementary.utils.time import DATETIME_FORMAT, convert_local_time_to_timezone

logger = get_logger(__name__)


class InvalidSelectorError(Exception):
    pass


class Status(str, Enum):
    WARN = "warn"
    FAIL = "fail"
    SKIPPED = "skipped"
    ERROR = "error"
    RUNTIME_ERROR = "runtime error"
    PARTIAL_SUCCESS = "partial success"


class ResourceType(str, Enum):
    TEST = "test"
    MODEL = "model"
    SOURCE_FRESHNESS = "source_freshness"


class FilterType(str, Enum):
    IS = "is"
    IS_NOT = "is_not"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"


class FilterFields(BaseModel):
    tags: List[str] = []
    models: List[str] = []
    owners: List[str] = []
    statuses: List[str] = []
    resource_types: List[ResourceType] = []
    node_names: List[str] = []
    test_ids: List[str] = []

    @property
    def normalized_status(self) -> List[Status]:
        return [Status(status) for status in self.statuses if status in list(Status)]


ValueT = TypeVar("ValueT")


ANY_OPERATORS = [FilterType.IS, FilterType.CONTAINS]
ALL_OPERATORS = [FilterType.IS_NOT, FilterType.NOT_CONTAINS]
NEGATIVE_OPERATORS = [FilterType.IS_NOT, FilterType.NOT_CONTAINS]


class FilterSchema(BaseModel, Generic[ValueT]):
    # The relation between values is OR.
    values: List[ValueT]
    type: FilterType = FilterType.IS

    class Config:
        # Make sure that serializing Enum return values
        use_enum_values = True

    @staticmethod
    def normalize_value(value: Any) -> str:
        if isinstance(value, Enum):
            return str(value.value).lower()
        return str(value).lower()

    @staticmethod
    def normalize_values(values: Iterable[ValueT]) -> Set[str]:
        return {FilterSchema.normalize_value(value) for value in values}

    @cached_property
    def _normalized_values(self) -> Set[str]:
        return FilterSchema.normalize_values(self.values)

    def get_matching_normalized_values(self, values: Set[str]) -> Set[str]:
        if self.type == FilterType.IS:
            return values.intersection(self._normalized_values)
        elif self.type == FilterType.IS_NOT:
            matching_values = values.difference(self._normalized_values)
            if len(matching_values) != len(values):
                return set()
            return matching_values
        if self.type == FilterType.CONTAINS:
            return set(
                value
                for value in values
                if any(
                    filter_value in str(value).lower()
                    for filter_value in self._normalized_values
                )
            )
        if self.type == FilterType.NOT_CONTAINS:
            matching_values = set(
                value
                for value in values
                if not any(
                    filter_value in str(value).lower()
                    for filter_value in self._normalized_values
                )
            )
            if len(matching_values) != len(values):
                return set()
            return matching_values
        raise ValueError(f"Unsupported filter type: {self.type}")

    def get_matching_values(self, values: Iterable[ValueT]) -> Set[str]:
        values_set = FilterSchema.normalize_values(values)
        return self.get_matching_normalized_values(values_set)

    def apply_filter_on_values(self, values: List[ValueT]) -> bool:
        if self.type in NEGATIVE_OPERATORS and not values:
            return True
        return bool(self.get_matching_values(values))

    def apply_filter_on_value(self, value: ValueT) -> bool:
        return self.apply_filter_on_values([value])


class StatusFilterSchema(FilterSchema[Status]):
    values: List[Status]


class ResourceTypeFilterSchema(FilterSchema[ResourceType]):
    values: List[ResourceType]


def _get_default_statuses_filter() -> List[StatusFilterSchema]:
    return [
        StatusFilterSchema(
            type=FilterType.IS,
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
    test_ids: List[FilterSchema[str]] = Field(default_factory=list)

    @validator("invocation_time", pre=True)
    def format_invocation_time(cls, invocation_time) -> Optional[str]:
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

    def validate_report_selector(self) -> None:
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
    def from_cli_params(
        cli_filters: Tuple[str],
        cli_excludes: Tuple[str],
        config: Optional[Any] = None,
        tracking: Optional[Any] = None,
    ) -> "FiltersSchema":
        from elementary.monitor.data_monitoring.selector_filter import SelectorFilter

        all_filters: list[tuple[str, FilterType]] = []
        for cli_filter in cli_filters:
            all_filters.append((cli_filter, FilterType.IS))
        for cli_exclude in cli_excludes:
            all_filters.append((cli_exclude, FilterType.IS_NOT))

        if not all_filters:
            return FiltersSchema()

        tags = []
        owners = []
        models = []
        statuses = []
        resource_types = []
        node_names = []

        for cli_filter, filter_type in all_filters:
            tags_match = FiltersSchema._match_filter_regex(
                filter_string=cli_filter, regex=re.compile(r"tags:(.*)")
            )
            if tags_match:
                tags.append(FilterSchema(values=tags_match, type=filter_type))
                continue

            owners_match = FiltersSchema._match_filter_regex(
                filter_string=cli_filter, regex=re.compile(r"owners:(.*)")
            )
            if owners_match:
                owners.append(FilterSchema(values=owners_match, type=filter_type))
                continue

            models_match = FiltersSchema._match_filter_regex(
                filter_string=cli_filter, regex=re.compile(r"models:(.*)")
            )
            if models_match:
                model_value = (
                    models_match[0]
                    if len(models_match) == 1
                    else ",".join(models_match)
                )
                if (
                    config
                    and filter_type == FilterType.IS
                    and SelectorFilter._has_graph_operators(model_value)
                ):
                    selector_filter = SelectorFilter(config, tracking, model_value)
                    filter_result = selector_filter.get_filter()
                    if filter_result.node_names:
                        node_names.extend(filter_result.node_names)
                else:
                    models.append(FilterSchema(values=models_match, type=filter_type))
                continue

            statuses_match = FiltersSchema._match_filter_regex(
                filter_string=cli_filter, regex=re.compile(r"statuses:(.*)")
            )
            if statuses_match:
                statuses.append(
                    StatusFilterSchema(
                        values=[Status(status) for status in statuses_match],
                        type=filter_type,
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
                        ],
                        type=filter_type,
                    )
                )
                continue

            logger.warning(
                f'Filter "{cli_filter.split(":")[0]}" is not supported - Skipping this filter ("{cli_filter}").'
            )

        if not any(status_filter.type == FilterType.IS for status_filter in statuses):
            statuses.extend(_get_default_statuses_filter())

        return FiltersSchema(
            tags=tags,
            owners=owners,
            models=models,
            statuses=statuses,
            resource_types=resource_types,
            node_names=node_names,
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

    def apply(
        self,
        filter_fields: FilterFields,
    ) -> bool:
        return (
            all(
                filter_schema.apply_filter_on_values(filter_fields.tags)
                for filter_schema in self.tags
            )
            and all(
                filter_schema.apply_filter_on_values(filter_fields.models)
                for filter_schema in self.models
            )
            and all(
                filter_schema.apply_filter_on_values(filter_fields.owners)
                for filter_schema in self.owners
            )
            and all(
                filter_schema.apply_filter_on_values(filter_fields.normalized_status)
                for filter_schema in self.statuses
            )
            and all(
                filter_schema.apply_filter_on_values(filter_fields.resource_types)
                for filter_schema in self.resource_types
            )
            and (
                FilterSchema(
                    values=self.node_names, type=FilterType.IS
                ).apply_filter_on_values(filter_fields.node_names)
                if self.node_names
                else True
            )
            and all(
                filter_schema.apply_filter_on_values(filter_fields.test_ids)
                for filter_schema in self.test_ids
            )
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
