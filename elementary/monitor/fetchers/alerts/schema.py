from datetime import datetime
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, validator

from elementary.utils.json_utils import (
    try_load_json,
    unpack_and_flatten_and_dedup_list_of_strings,
    unpack_and_flatten_str_to_list,
)

ALERTS_CONFIG_KEY = "alerts_config"
CHANNEL_KEY = "channel"
DESCRIPTION_KEY = "description"
OWNERS_KEY = "owners"
SUBSCRIBERS_KEY = "subscribers"
CHANNEL_KEY = "channel"
ALERT_FIELDS_KEY = "alert_fields"
ALERT_SUPPRESSION_INTERVAL_KEY = "alert_suppression_interval"
GROUP_ALERTS_BY_KEY = "slack_group_alerts_by"


class BasePendingAlertSchema(BaseModel):
    id: str
    alert_class_id: str
    model_unique_id: Optional[str] = None
    detected_at: datetime
    database_name: str
    schema_name: str
    tags: Optional[List[str]] = None
    model_meta: Optional[Dict] = None
    suppression_status: str
    sent_at: Optional[datetime] = None
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
    def owners(self) -> List[str]:
        return self._get_alert_meta_attrs(OWNERS_KEY)

    @property
    def subscribers(self) -> List[str]:
        return self._get_alert_meta_attrs(SUBSCRIBERS_KEY)

    @property
    def description(self) -> Optional[str]:
        return self.unified_meta.get(DESCRIPTION_KEY)

    @property
    def alert_fields(self) -> List[str]:
        return self.unified_meta.get(ALERT_FIELDS_KEY, [])

    @property
    def alert_channel(self) -> Optional[str]:
        return self.unified_meta.get(CHANNEL_KEY)

    @validator("model_meta", pre=True, always=True)
    def validate_model_meta(cls, model_meta: Optional[Dict]) -> Dict:
        return cls._validate_dict(model_meta)

    @validator("tags", pre=True, always=True)
    def validate_tags(cls, tags: Optional[Union[List[str], str]]):
        return unpack_and_flatten_and_dedup_list_of_strings(tags)

    @staticmethod
    def _flatten_meta(meta: Optional[Dict] = None) -> Dict:
        unflatten_meta = meta or dict()
        # backwards compatibility for alert configuration
        flatten_meta = {**unflatten_meta, **unflatten_meta.get(ALERTS_CONFIG_KEY, {})}
        flatten_meta.pop(ALERTS_CONFIG_KEY, None)
        return flatten_meta

    def _get_alert_meta_attrs(self, meta_key: str) -> List[str]:
        attrs = []
        model_attrs = self.flatten_model_meta.get(meta_key, [])
        if isinstance(model_attrs, list):
            attrs.extend(model_attrs)
        elif isinstance(model_attrs, str):
            attrs.append(model_attrs)
        return unpack_and_flatten_and_dedup_list_of_strings(attrs)

    def get_suppression_interval(
        self,
        interval_from_cli: int,
        override_by_cli: bool = False,
    ) -> int:
        interval_from_alert = self.alert_suppression_interval
        if interval_from_alert is None or override_by_cli:
            return interval_from_cli
        return interval_from_alert

    @staticmethod
    def _validate_dict(value: Optional[Dict]) -> Dict:
        if not value:
            return dict()
        return try_load_json(value)


class PendingTestAlertSchema(BasePendingAlertSchema):
    test_unique_id: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    test_type: str
    test_sub_type: str
    test_results_description: str
    test_results_query: Optional[str] = None
    test_rows_sample: Optional[List[Dict]] = None
    other: Optional[Dict] = None
    test_name: str
    test_short_name: str
    test_params: Optional[Dict] = None
    severity: str
    test_meta: Optional[Dict] = None
    elementary_unique_id: str

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

    def get_alert_meta_attrs(self, meta_key: str) -> List[str]:
        attrs = []
        test_attrs = self.flatten_test_meta.get(meta_key, [])
        model_attrs = self.flatten_model_meta.get(meta_key, [])
        if isinstance(test_attrs, list):
            attrs.extend(test_attrs)
        elif isinstance(test_attrs, str):
            attrs.append(test_attrs)

        if isinstance(model_attrs, list):
            attrs.extend(model_attrs)
        elif isinstance(model_attrs, str):
            attrs.append(model_attrs)
        return unpack_and_flatten_and_dedup_list_of_strings(attrs)


class PendingModelAlertSchema(BasePendingAlertSchema):
    alias: str
    path: str
    original_path: str
    materialization: str
    full_refresh: bool
    message: str


class PendingSourceFreshnessAlertSchema(BasePendingAlertSchema):
    snapshotted_at: datetime
    max_loaded_at: datetime
    max_loaded_at_time_ago_in_s: int
    source_name: str
    identifier: str
    error_after: Optional[str] = None
    warn_after: Optional[str] = None
    filter: Optional[str] = None
    normalized_status: str
    path: str
    error: str
    freshness_description: Optional[str] = None
