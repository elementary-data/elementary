import json

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.monitor.alerts.malformed import MalformedAlert

MOCK_MALFORMED_ALERT = MalformedAlert(
    id="1",
    data=dict(
        id="1",
        alert_class_id="test_id_1",
        model_unique_id="elementary.model_id_1",
        test_unique_id="test_id_1",
        test_name="test_1",
        test_created_at="2022-10-10 10:10:10",
        tags='["one", "two"]',
        owners='["jeff", "john"]',
    ),
)


def test_to_slack():
    malformed_alert = MOCK_MALFORMED_ALERT
    slack_message = malformed_alert.to_slack()
    assert isinstance(slack_message, SlackMessageSchema)
    assert json.dumps(malformed_alert.data, indent=2) in slack_message.text


def test_malformed_alert_getattribute():
    malformed_alert = MOCK_MALFORMED_ALERT
    assert malformed_alert.alert_class_id == "test_id_1"
    assert malformed_alert.unique_id is None
