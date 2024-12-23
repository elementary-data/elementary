from datetime import datetime
from enum import Enum
from typing import Optional, Union

from elementary.monitor.fetchers.alerts.schema.alert_data import (
    ModelAlertDataSchema,
    SourceFreshnessAlertDataSchema,
    TestAlertDataSchema,
)
from elementary.utils.json_utils import try_load_json
from elementary.utils.pydantic_shim import BaseModel, root_validator

ALERTS_CONFIG_KEY = "alerts_config"
CHANNEL_KEY = "channel"
DESCRIPTION_KEY = "description"
OWNER_KEY = "owner"
SUBSCRIBERS_KEY = "subscribers"
ALERT_FIELDS_KEY = "alert_fields"
ALERT_SUPPRESSION_INTERVAL_KEY = "alert_suppression_interval"
GROUP_ALERTS_BY_KEY = "slack_group_alerts_by"


class AlertTypes(Enum):
    TEST = "test"
    MODEL = "model"
    SOURCE_FRESHNESS = "source_freshness"


class AlertStatus(Enum):
    PENDING = "pending"
    SENT = "sent"
    SKIPPED = "skipped"


class PendingAlertSchema(BaseModel):
    id: str
    alert_class_id: str
    type: AlertTypes
    detected_at: datetime
    created_at: datetime
    updated_at: datetime
    status: AlertStatus
    data: Union[
        TestAlertDataSchema, ModelAlertDataSchema, SourceFreshnessAlertDataSchema
    ]
    sent_at: Optional[datetime] = None

    class Config:
        smart_union = True
        # Make sure that serializing Enum return values
        use_enum_values = True

    @root_validator(pre=True)
    def validate_times(cls, values: dict) -> dict:
        new_values = {**values}

        current_datetime = datetime.utcnow()
        if not values.get("detected_at"):
            new_values["detected_at"] = current_datetime
        if not values.get("created_at"):
            new_values["created_at"] = current_datetime
        if not values.get("updated_at"):
            new_values["updated_at"] = current_datetime
        return new_values

    @root_validator(pre=True)
    def parse_data(cls, values: dict) -> dict:
        new_values = {**values}

        alert_type = AlertTypes(values.get("type"))
        data = values.get("data")

        if (
            alert_type is AlertTypes.TEST
            and isinstance(data, TestAlertDataSchema)
            or alert_type is AlertTypes.MODEL
            and isinstance(data, ModelAlertDataSchema)
            or alert_type is AlertTypes.SOURCE_FRESHNESS
            and isinstance(data, SourceFreshnessAlertDataSchema)
        ):
            return values

        raw_data = try_load_json(values.get("data"))

        data = None
        if alert_type is AlertTypes.TEST:
            data = TestAlertDataSchema(**raw_data)
        elif alert_type is AlertTypes.MODEL:
            data = ModelAlertDataSchema(**raw_data)  # type: ignore[assignment]
        elif alert_type is AlertTypes.SOURCE_FRESHNESS:
            data = SourceFreshnessAlertDataSchema(**raw_data)  # type: ignore[assignment]

        new_values["data"] = data
        return new_values
