import copy
from typing import Any, List, Optional

from elementary.utils.json_utils import (
    try_load_json,
    unpack_and_flatten_and_dedup_list_of_strings,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)

TABLE_FIELD = "table"
COLUMN_FIELD = "column"
DESCRIPTION_FIELD = "description"
OWNERS_FIELD = "owners"
TAGS_FIELD = "tags"
SUBSCRIBERS_FIELD = "subscribers"
RESULT_MESSAGE_FIELD = "result_message"
TEST_PARAMS_FIELD = "test_parameters"
TEST_QUERY_FIELD = "test_query"
TEST_RESULTS_SAMPLE_FIELD = "test_results_sample"
DEFAULT_ALERT_FIELDS = [
    TABLE_FIELD,
    COLUMN_FIELD,
    DESCRIPTION_FIELD,
    OWNERS_FIELD,
    TAGS_FIELD,
    SUBSCRIBERS_FIELD,
    RESULT_MESSAGE_FIELD,
    TEST_PARAMS_FIELD,
    TEST_QUERY_FIELD,
    TEST_RESULTS_SAMPLE_FIELD,
]

TEST_META_KEY = "test_meta"
MODEL_META_KEY = "model_meta"
ALERTS_CONFIG_KEY = "alerts_config"
OWNERS_KEY = "owners"
SUBSCRIBERS_KEY = "subscribers"
CHANNEL_KEY = "channel"
ALERT_FIELDS_KEY = "alert_fields"
ALERT_SUPPRESSION_INTERVAL_KEY = "alert_suppression_interval"
SLACK_GROUP_ALERTS_BY_KEY = "slack_group_alerts_by"


class NormalizedAlert:
    def __init__(self, alert: dict) -> None:
        self.alert = alert
        self.test_meta = self._flatten_meta(TEST_META_KEY)
        self.model_meta = self._flatten_meta(MODEL_META_KEY)
        self.normalized_alert = self._normalize_alert(self.model_meta, self.test_meta)

    def get_normalized_alert(self) -> dict:
        return self.normalized_alert

    def _flatten_meta(self, node_meta_field: str) -> dict:
        # backwards compatibility for alert configuration
        meta = try_load_json(self.alert.get(node_meta_field)) or {}
        flatten_meta = {**meta, **meta.get(ALERTS_CONFIG_KEY, {})}
        flatten_meta.pop(ALERTS_CONFIG_KEY, None)
        return flatten_meta

    def _normalize_alert(
        self, flattened_model_meta: dict, flattened_test_meta: dict
    ) -> dict:
        """
        extract from the test+model jsons:
        key in data ( or default val)  -- > key in normalized alert
        SUBSCRIBERS_KEY (or []) --> SUBSCRIBERS_KEY
        "owner" (or []) --> OWNERS_KEY
        CHANNEL_KEY (or None) --> "Slack channel"
        ALERT_FIELDS_KEY (or DEFAULT_ALERT_FIELDS)--> ALERT_FIELDS_KEY
        "group" (or None) --> SLACK_GROUP_ALERTS_BY_KEY

        After this normalization step, Tags, Owners and Subscribers should all be deduplicated Lists, either of strings or []


        :return:
        """

        try:
            normalized_alert = copy.deepcopy(self.alert)
            normalized_alert[MODEL_META_KEY] = flattened_model_meta
            normalized_alert[TEST_META_KEY] = flattened_test_meta

            normalized_alert[
                SUBSCRIBERS_KEY
            ] = unpack_and_flatten_and_dedup_list_of_strings(
                self._get_alert_meta_attrs(SUBSCRIBERS_KEY)
            )
            normalized_alert[OWNERS_KEY] = unpack_and_flatten_and_dedup_list_of_strings(
                normalized_alert.get(OWNERS_KEY)
            )
            normalized_alert[TAGS_FIELD] = unpack_and_flatten_and_dedup_list_of_strings(
                normalized_alert.get(TAGS_FIELD)
            )

            normalized_alert["slack_channel"] = self._get_alert_channel()
            normalized_alert[
                ALERT_SUPPRESSION_INTERVAL_KEY
            ] = self._get_alert_suppression_interval()
            normalized_alert[ALERT_FIELDS_KEY] = self._get_alert_fields()

            normalized_alert[
                SLACK_GROUP_ALERTS_BY_KEY
            ] = self._get_field_from_test_meta_or_model_meta_or_default(
                key=SLACK_GROUP_ALERTS_BY_KEY
            )

            return normalized_alert
        except Exception:
            logger.error(
                f"Failed to extract alert subscribers and alert custom slack channel {self.alert.get('id')}. Ignoring it for now and main slack channel will be used"
            )
            return self.alert

    def _get_alert_meta_attrs(self, meta_key: str) -> List[str]:
        attrs = []
        test_attrs = self.test_meta.get(meta_key, [])
        model_attrs = self.model_meta.get(meta_key, [])
        if isinstance(test_attrs, list):
            attrs.extend(test_attrs)
        elif isinstance(test_attrs, str):
            attrs.append(test_attrs)

        if isinstance(model_attrs, list):
            attrs.extend(model_attrs)
        elif isinstance(model_attrs, str):
            attrs.append(model_attrs)
        return attrs

    def _get_alert_channel(self) -> Optional[str]:
        return self._get_field_from_test_meta_or_model_meta_or_default(key=CHANNEL_KEY)

    def _get_alert_suppression_interval(self) -> Optional[int]:
        return self._get_field_from_test_meta_or_model_meta_or_default(
            key=ALERT_SUPPRESSION_INTERVAL_KEY, default_val=None
        )

    def _get_alert_fields(self) -> Optional[List[str]]:
        # If there is no alerts_fields in the test meta object,
        # we return the model alerts_fields from the model meta object.
        # The fallback is DEFAULT_ALERT_FIELDS.
        return self._get_field_from_test_meta_or_model_meta_or_default(
            key=ALERT_FIELDS_KEY, default_val=DEFAULT_ALERT_FIELDS
        )

    def _get_field_from_test_meta_or_model_meta_or_default(
        self, key: str, default_val=None
    ) -> Any:
        if self.test_meta.get(key) is not None:
            return self.test_meta.get(key)
        if self.model_meta.get(key) is not None:
            return self.model_meta.get(key)
        return default_val
