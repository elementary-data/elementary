import json

from elementary.monitor.alerts.group_of_alerts import GroupingType
from elementary.monitor.fetchers.alerts.normalized_alert import CHANNEL_KEY
from tests.unit.monitor.alerts.group_alerts_by_table.mock_classes import MockAlert

DEFAULT_CHANNEL = "roi-playground"
OTHER_CHANNEL = "roi-playground-2"
MODEL_1 = "models.bla.model1"
MODEL_2 = "models.bla.model2"
MODEL_3 = "models.bla.model3"
OWNER_1 = "owner1@elementary-data.com"
OWNER_2 = "owner2@elementary-data.com"
OWNER_3 = "owner3@elementary-data.com"
TAGS_1 = ["marketing", "finance"]
TAGS_2 = ["finance", "data-ops"]
TAGS_3 = ["#marketing", "#finance", "#data-ops"]
DETECTED_AT = "1992-11-11 20:00:03+0200"
EMPTY_DICT = json.dumps(dict())
GROUP_BY_ALERT_IN_DICT = json.dumps({"group_alerts_by": GroupingType.BY_ALERT.value})
OTHER_CHANNEL_IN_DICT = json.dumps({CHANNEL_KEY: OTHER_CHANNEL})
AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING = MockAlert(
    status="warn",
    slack_group_alerts_by=None,
    model_unique_id=MODEL_1,
    slack_channel=None,
    detected_at=DETECTED_AT,
    model_meta=EMPTY_DICT,
    owners=[OWNER_1, OWNER_2],
    subscribers=[],
    tags=TAGS_1,
)
AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING = MockAlert(
    status="fail",
    slack_group_alerts_by=None,
    model_unique_id=MODEL_1,
    slack_channel=OTHER_CHANNEL,
    detected_at=DETECTED_AT,
    model_meta="{}",
    owners=[OWNER_1, OWNER_3],
    subscribers=[],
    tags=TAGS_2,
)
AL_FAIL_MODEL2_NO_CHANNEL_NO_GROUPING = MockAlert(
    status="fail",
    slack_group_alerts_by=None,
    model_unique_id=MODEL_2,
    slack_channel=None,
    detected_at=DETECTED_AT,
    model_meta=EMPTY_DICT,
    owners=[OWNER_1],
    subscribers=[],
    tags=TAGS_3,
)
AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_ALERT = MockAlert(
    status="fail",
    slack_group_alerts_by="alert",
    model_unique_id=MODEL_2,
    slack_channel=None,
    detected_at=DETECTED_AT,
    model_meta=EMPTY_DICT,
    owners=[OWNER_1],
    subscribers=[],
    tags=TAGS_1,
)
AL_WARN_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_ALERT = MockAlert(
    status="warn",
    slack_group_alerts_by="alert",
    model_unique_id=MODEL_2,
    slack_channel=None,
    detected_at=DETECTED_AT,
    model_meta=EMPTY_DICT,
    owners=[OWNER_1],
    subscribers=[],
    tags=TAGS_1,
)
AL_FAIL_MODEL2_WITH_CHANNEL_IN_MODEL_META_WITH_GROUPING_BY_TABLE = MockAlert(
    status="fail",
    slack_group_alerts_by="table",
    model_unique_id=MODEL_2,
    slack_channel=None,
    detected_at=DETECTED_AT,
    model_meta=OTHER_CHANNEL_IN_DICT,
    owners=[OWNER_1],
    subscribers=[],
    tags=TAGS_1,
)
AL_ERROR_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE = MockAlert(
    status="error",
    slack_group_alerts_by="table",
    model_unique_id=MODEL_2,
    slack_channel=None,
    detected_at=DETECTED_AT,
    model_meta=EMPTY_DICT,
    owners=[OWNER_1],
    subscribers=[],
    tags=TAGS_1,
)
AL_ERROR_MODEL3_NO_CHANNEL_WITH_MODEL_META_GROUPING_BY_ALERT = MockAlert(
    status="error",
    slack_group_alerts_by="alert",
    model_unique_id=MODEL_3,
    slack_channel=None,
    detected_at=DETECTED_AT,
    model_meta=GROUP_BY_ALERT_IN_DICT,
    owners=[OWNER_1],
    subscribers=[],
    tags=TAGS_1,
)
