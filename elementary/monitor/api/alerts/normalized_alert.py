import copy
from typing import List, Optional

from networkx import NetworkXError

from elementary.monitor.api.lineage.schema import LineageSchema
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger

logger = get_logger(__name__)

TABLE_FIELD = "table"
COLUMN_FIELD = "column"
DESCRIPTION_FIELD = "description"
OWNERS_FIELD = "owners"
TAGS_FIELD = "tags"
SUBSCRIBERS_FIELD = "subscribers"
EXPOSURES_FIELD = "exposures"
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
    EXPOSURES_FIELD,
    RESULT_MESSAGE_FIELD,
    TEST_PARAMS_FIELD,
    TEST_QUERY_FIELD,
    TEST_RESULTS_SAMPLE_FIELD,
]

TEST_META_KEY = "test_meta"
MODEL_META_KEY = "model_meta"
ALERTS_CONFIG_KEY = "alerts_config"
SUBSCRIBERS_KEY = "subscribers"
CHANNEL_KEY = "channel"
ALERT_FIELDS_KEY = "alert_fields"
ALERT_SUPRESSION_INTERVAL_KEY = "alert_suppression_interval"


class NormalizedAlert:
    def __init__(self, alert: dict, lineage: LineageSchema) -> None:
        self.alert = alert
        self.test_meta = self._flatten_meta(TEST_META_KEY)
        self.model_meta = self._flatten_meta(MODEL_META_KEY)
        self.lineage = lineage

    def as_dict(self) -> dict:
        return self._normalize_alert_dict()

    def _flatten_meta(self, node_meta_field: str) -> dict:
        # backwards compatibility for alert configuration
        meta = try_load_json(self.alert.get(node_meta_field)) or {}
        flatten_meta = {**meta, **meta.get(ALERTS_CONFIG_KEY, {})}
        flatten_meta.pop(ALERTS_CONFIG_KEY, None)
        return flatten_meta

    def _normalize_alert_dict(self):
        normalized_alert = copy.deepcopy(self.alert)
        normalized_alert[SUBSCRIBERS_KEY] = self._get_alert_subscribers()
        normalized_alert["slack_channel"] = self._get_alert_chennel()
        normalized_alert[
            ALERT_SUPRESSION_INTERVAL_KEY
        ] = self._get_alert_suppression_interval()
        normalized_alert[ALERT_FIELDS_KEY] = self._get_alert_fields()
        normalized_alert["affected_exposures"] = self._get_affected_exposures()
        return normalized_alert

    def _get_alert_subscribers(self) -> List[Optional[str]]:
        subscribers = []
        test_subscribers = self.test_meta.get(SUBSCRIBERS_KEY, [])
        model_subscribers = self.model_meta.get(SUBSCRIBERS_KEY, [])
        if isinstance(test_subscribers, list):
            subscribers.extend(test_subscribers)
        else:
            subscribers.append(test_subscribers)

        if isinstance(model_subscribers, list):
            subscribers.extend(model_subscribers)
        else:
            subscribers.append(model_subscribers)
        return subscribers

    def _get_alert_chennel(self) -> Optional[str]:
        model_slack_channel = self.model_meta.get(CHANNEL_KEY)
        test_slack_channel = self.test_meta.get(CHANNEL_KEY)
        return test_slack_channel or model_slack_channel

    def _get_alert_suppression_interval(self) -> int:
        model_alert_suppression_interval = self.model_meta.get(
            ALERT_SUPRESSION_INTERVAL_KEY
        )
        test_alert_suppression_interval = self.test_meta.get(
            ALERT_SUPRESSION_INTERVAL_KEY
        )
        if test_alert_suppression_interval is not None:
            return test_alert_suppression_interval
        elif model_alert_suppression_interval is not None:
            return model_alert_suppression_interval
        else:
            return 0

    def _get_alert_fields(self) -> Optional[List[str]]:
        # If there is no alerts_fields in the test meta object,
        # we return the model alerts_fields from the model meta object.
        # The fallback is DEFAULT_ALERT_FIELDS.
        return (
            self.test_meta.get(ALERT_FIELDS_KEY)
            or self.model_meta.get(ALERT_FIELDS_KEY)
            or DEFAULT_ALERT_FIELDS
        )

    def _get_affected_exposures(self) -> List[str]:
        alert_node = self.alert.get("model_unique_id") or self.alert.get("unique_id")
        exposures = [node.id for node in self.lineage.nodes if node.type == "exposure"]
        try:
            downstream_nodes = self.lineage.graph.predecessors(alert_node)
        except NetworkXError:
            return []
        return list(set(exposures) & set(downstream_nodes))
