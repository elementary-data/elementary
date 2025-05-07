from datetime import datetime
from typing import Dict, List, Optional, Union

from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.schema import ResourceType
from elementary.utils.dicts import flatten_dict_by_key, merge_dicts_attribute
from elementary.utils.json_utils import (
    try_load_json,
    unpack_and_flatten_and_dedup_list_of_strings,
    unpack_and_flatten_str_to_list,
)
from elementary.utils.pydantic_shim import BaseModel, Field, validator

ALERTS_CONFIG_KEY = "alerts_config"
CHANNEL_KEY = "channel"
DESCRIPTION_KEY = "description"
OWNER_KEY = "owner"
SUBSCRIBERS_KEY = "subscribers"
ALERT_FIELDS_KEY = "alert_fields"
ALERT_SUPPRESSION_INTERVAL_KEY = "alert_suppression_interval"
GROUP_ALERTS_BY_KEY = "slack_group_alerts_by"


class BaseAlertDataSchema(BaseModel):
    id: str
    alert_class_id: str
    model_unique_id: Optional[str] = None
    detected_at: datetime
    database_name: Optional[str] = None
    schema_name: str
    tags: Optional[List[str]] = None
    owners: Optional[List[str]] = None
    model_meta: Optional[Dict] = None
    status: str

    @property
    def unified_meta(self) -> Dict:
        return self.flatten_model_meta

    @property
    def flatten_model_meta(self) -> Dict:
        return self._flatten_meta(self.model_meta)

    @property
    def alert_suppression_interval(self) -> Optional[int]:
        return self.unified_meta.get(ALERT_SUPPRESSION_INTERVAL_KEY)

    @property
    def group_alerts_by(self) -> Optional[str]:
        return self.unified_meta.get(GROUP_ALERTS_BY_KEY)

    @property
    def unified_owners(self) -> List[str]:
        # Make sure we return both meta defined owners and config defined owners.
        config_owners = self.owners or []
        meta_owners = self._get_alert_meta_attrs(OWNER_KEY)
        return list(set(config_owners + meta_owners))

    @property
    def subscribers(self) -> List[str]:
        return self._get_alert_meta_attrs(SUBSCRIBERS_KEY)

    @property
    def description(self) -> Optional[str]:
        return self.unified_meta.get(DESCRIPTION_KEY)

    @property
    def alert_fields(self) -> List[str]:
        return self.unified_meta.get(ALERT_FIELDS_KEY, [])

    @validator("model_meta", pre=True, always=True)
    def validate_model_meta(cls, model_meta: Optional[Dict]) -> Dict:
        return cls._validate_dict(model_meta)

    @validator("tags", pre=True, always=True)
    def validate_tags(cls, tags: Optional[Union[List[str], str]]):
        return unpack_and_flatten_and_dedup_list_of_strings(tags)

    @validator("owners", pre=True, always=True)
    def validate_owners(cls, owners: Optional[Union[List[str], str]]):
        return unpack_and_flatten_and_dedup_list_of_strings(owners)

    @staticmethod
    def _flatten_meta(meta: Optional[Dict] = None) -> Dict:
        return flatten_dict_by_key(meta, ALERTS_CONFIG_KEY) if meta else dict()

    def _get_alert_meta_attrs(self, meta_key: str) -> List[str]:
        attrs: List[str] = merge_dicts_attribute(
            dicts=[self.flatten_model_meta], attribute_key=meta_key
        )
        return unpack_and_flatten_and_dedup_list_of_strings(attrs)

    @staticmethod
    def _validate_dict(value: Optional[Dict]) -> Dict:
        if not value:
            return dict()
        return try_load_json(value)

    def format_alert(
        self,
        timezone: Optional[str] = None,
        report_url: Optional[str] = None,
        elementary_database_and_schema: Optional[str] = None,
        global_suppression_interval: int = 0,
        override_config: bool = False,
        env: Optional[str] = None,
        *args,
        **kwargs
    ):
        raise NotImplementedError

    def get_suppression_interval(
        self,
        interval_from_cli: int,
        override_by_cli: bool = False,
    ) -> int:
        interval_from_alert = self.alert_suppression_interval
        if interval_from_alert is None or override_by_cli:
            return interval_from_cli
        return interval_from_alert


class TestAlertDataSchema(BaseAlertDataSchema):
    __test__ = False  # Mark for pytest - The class name starts with "Test" which throws warnings on pytest runs

    test_unique_id: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    test_type: str
    test_sub_type: str
    test_description: Optional[str] = None
    test_results_description: Optional[str] = None
    test_results_query: Optional[str] = None
    test_rows_sample: Optional[List[Dict]] = None
    other: Optional[Dict] = None
    test_name: str
    test_short_name: str
    test_params: Optional[Dict] = None
    severity: str
    test_meta: Optional[Dict] = None
    elementary_unique_id: str
    resource_type: ResourceType = Field(ResourceType.TEST, const=True)  # type: ignore  # noqa

    @property
    def flatten_model_meta(self) -> Dict:
        return self._flatten_meta(self.model_meta)

    @property
    def flatten_test_meta(self) -> Dict:
        return self._flatten_meta(self.test_meta)

    @property
    def unified_meta(self) -> Dict:
        return {**self.flatten_model_meta, **self.flatten_test_meta}

    @validator("test_rows_sample", pre=True, always=True)
    def validate_test_rows_sample(cls, test_rows_sample):
        if not test_rows_sample:
            return []
        return unpack_and_flatten_str_to_list(test_rows_sample)

    @validator("test_params", pre=True, always=True)
    def validate_test_params(cls, test_params: Optional[Dict]) -> Dict:
        return cls._validate_dict(test_params)

    @validator("test_meta", pre=True, always=True)
    def validate_test_meta(cls, test_meta: Optional[Dict]) -> Dict:
        return cls._validate_dict(test_meta)

    @validator("other", pre=True, always=True)
    def validate_other(cls, other: Optional[Dict]) -> Dict:
        return cls._validate_dict(other)

    def _get_alert_meta_attrs(self, meta_key: str) -> List[str]:
        attrs: List[str] = merge_dicts_attribute(
            dicts=[self.flatten_model_meta, self.flatten_test_meta],
            attribute_key=meta_key,
        )
        return unpack_and_flatten_and_dedup_list_of_strings(attrs)

    def format_alert(
        self,
        timezone: Optional[str] = None,
        report_url: Optional[str] = None,
        elementary_database_and_schema: Optional[str] = None,
        global_suppression_interval: int = 0,
        override_config: bool = False,
        env: Optional[str] = None,
        disable_samples: bool = False,
        *args,
        **kwargs
    ) -> TestAlertModel:
        return TestAlertModel(
            id=self.id,
            test_unique_id=self.test_unique_id,
            elementary_unique_id=self.elementary_unique_id,
            test_name=self.test_name,
            severity=self.severity,
            table_name=self.table_name,
            test_type=self.test_type,
            test_sub_type=self.test_sub_type,
            test_results_description=self.test_results_description,
            test_results_query=self.test_results_query,
            test_short_name=self.test_short_name,
            test_description=self.description or self.test_description,
            other=self.other,
            test_params=self.test_params,
            test_rows_sample=self.test_rows_sample if not disable_samples else None,
            column_name=self.column_name,
            alert_class_id=self.alert_class_id,
            model_unique_id=self.model_unique_id,
            detected_at=self.detected_at,
            database_name=self.database_name,
            schema_name=self.schema_name,
            owners=self.unified_owners,
            tags=self.tags,
            subscribers=self.subscribers,
            status=self.status,
            model_meta=self.flatten_model_meta,
            test_meta=self.flatten_test_meta,
            suppression_interval=self.get_suppression_interval(
                global_suppression_interval, override_config
            ),
            timezone=timezone,
            report_url=report_url,
            alert_fields=self.alert_fields,
            elementary_database_and_schema=elementary_database_and_schema,
            env=env,
        )


class ModelAlertDataSchema(BaseAlertDataSchema):
    alias: str
    path: str
    original_path: str
    materialization: str
    full_refresh: bool
    message: Optional[str] = None
    resource_type: ResourceType = Field(ResourceType.MODEL, const=True)  # type: ignore  # noqa

    def format_alert(
        self,
        timezone: Optional[str] = None,
        report_url: Optional[str] = None,
        elementary_database_and_schema: Optional[str] = None,
        global_suppression_interval: int = 0,
        override_config: bool = False,
        env: Optional[str] = None,
        *args,
        **kwargs
    ) -> ModelAlertModel:
        return ModelAlertModel(
            id=self.id,
            alias=self.alias,
            path=self.path,
            original_path=self.original_path,
            materialization=self.materialization,
            message=self.message,
            full_refresh=self.full_refresh,
            alert_class_id=self.alert_class_id,
            model_unique_id=self.model_unique_id,
            detected_at=self.detected_at,
            database_name=self.database_name,
            schema_name=self.schema_name,
            owners=self.unified_owners,
            tags=self.tags,
            subscribers=self.subscribers,
            status=self.status,
            model_meta=self.flatten_model_meta,
            suppression_interval=self.get_suppression_interval(
                global_suppression_interval, override_config
            ),
            timezone=timezone,
            report_url=report_url,
            alert_fields=self.alert_fields,
            elementary_database_and_schema=elementary_database_and_schema,
            env=env,
        )

    @validator("full_refresh", pre=True, always=True)
    def validate_full_refresh(cls, full_refresh: Optional[bool]) -> bool:
        if full_refresh is None:
            return False
        return full_refresh


class SourceFreshnessAlertDataSchema(BaseAlertDataSchema):
    source_freshness_execution_id: str
    snapshotted_at: Optional[datetime] = None
    max_loaded_at: Optional[datetime] = None
    max_loaded_at_time_ago_in_s: Optional[int] = None
    source_name: str
    identifier: str
    error_after: Optional[str] = None
    warn_after: Optional[str] = None
    filter: Optional[str] = None
    original_status: str
    path: str
    error: Optional[str] = None
    freshness_description: Optional[str] = None
    resource_type: ResourceType = Field(ResourceType.SOURCE_FRESHNESS, const=True)  # type: ignore  # noqa

    def format_alert(
        self,
        timezone: Optional[str] = None,
        report_url: Optional[str] = None,
        elementary_database_and_schema: Optional[str] = None,
        global_suppression_interval: int = 0,
        override_config: bool = False,
        env: Optional[str] = None,
        *args,
        **kwargs
    ) -> SourceFreshnessAlertModel:
        return SourceFreshnessAlertModel(
            id=self.id,
            source_name=self.source_name,
            identifier=self.identifier,
            status=self.status,
            original_status=self.original_status,
            error_after=self.error_after,
            warn_after=self.warn_after,
            path=self.path,
            error=self.error,
            source_freshness_execution_id=self.source_freshness_execution_id,
            snapshotted_at=self.snapshotted_at,
            max_loaded_at=self.max_loaded_at,
            max_loaded_at_time_ago_in_s=self.max_loaded_at_time_ago_in_s,
            filter=self.filter,
            freshness_description=self.freshness_description,
            alert_class_id=self.alert_class_id,
            model_unique_id=self.model_unique_id,
            detected_at=self.detected_at,
            database_name=self.database_name,
            schema_name=self.schema_name,
            owners=self.unified_owners,
            tags=self.tags,
            subscribers=self.subscribers,
            model_meta=self.flatten_model_meta,
            suppression_interval=self.get_suppression_interval(
                global_suppression_interval, override_config
            ),
            timezone=timezone,
            report_url=report_url,
            alert_fields=self.alert_fields,
            elementary_database_and_schema=elementary_database_and_schema,
            env=env,
        )
