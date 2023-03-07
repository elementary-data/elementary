from collections import Counter

import pytest

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.config.config import Config
from elementary.monitor.alerts.group_of_alerts import GroupingType
from tests.mocks.anonymous_tracking_mock import MockAnonymousTracking
from tests.mocks.data_monitoring_alerts_mock import DataMonitoringAlertsMock
from tests.mocks.slack_client_mock import SlackWebClientMock

SLACK_CHANNEL_TEST_NOT_USED_NAME = "test"

# these will need to be updated when the package e2e changes.
NUM_ALERTS_E2E_GIVES = {"snowflake": 127, "postgres": 132}
EXPECTED_MESSAGE_HEADERS_COUNT_SNOWFLAKE = [
    (":small_red_triangle: Data anomaly detected", 104),
    (":small_red_triangle: Schema change detected", 11),
    (":small_red_triangle: dbt test alert", 7),
    (":warning: Data anomaly detected", 1),
    (":x: Schema change detected", 1),
    (":x: dbt model alert", 1),
    (":x: dbt snapshot alert", 1),
    (":x: dbt test alert", 1),
]
EXPECTED_MESSAGE_HEADERS_COUNT_POSTGRES = [
    (":small_red_triangle: Data anomaly detected", 109),
    (":small_red_triangle: Schema change detected", 11),
    (":small_red_triangle: dbt test alert", 7),
    (":warning: Data anomaly detected", 1),
    (":x: Schema change detected", 1),
    (":x: dbt model alert", 1),
    (":x: dbt snapshot alert", 1),
    (":x: dbt test alert", 1),
]
EXPECTED_MESSAGE_HEADERS_COUNT = {
    "snoflake": EXPECTED_MESSAGE_HEADERS_COUNT_SNOWFLAKE,
    "postgres": EXPECTED_MESSAGE_HEADERS_COUNT_POSTGRES,
}
TABLES_TO_UPDATE_IN_EDR_RUN_ON_E2E = {
    "snowflake": ["alerts_models", "alerts"],
    "postgres": ["alerts_models", "alerts"],
}
NUM_ALERTS_E2E_GIVES["bigquery"] = 132
TABLES_TO_UPDATE_IN_EDR_RUN_ON_E2E["bigquery"] = ["alerts_models", "alerts"]
EXPECTED_MESSAGE_HEADERS_COUNT["bigquery"] = [
    (":small_red_triangle: Data anomaly detected", 109),
    (":small_red_triangle: Schema change detected", 11),
    (":small_red_triangle: dbt test alert", 7),
    (":warning: Data anomaly detected", 1),
    (":x: Schema change detected", 1),
    (":x: dbt model alert", 1),
    (":x: dbt snapshot alert", 1),
    (":x: dbt test alert", 1),
]


def try_parse_header_text_from_slack_message_schema(msg: SlackMessageSchema):
    # find header block
    header_block = None
    for bl in msg.blocks:
        if bl.get("type") == "header":
            header_block = bl
    if not header_block:
        return None
    header_text_part = header_block.get("text")
    if not header_text_part:
        return None
    return header_text_part.get("text")


def test_alerts(warehouse_type, days_back=1):
    # very similar to the CLI, only mocking edr monitor:
    # - the part where alerts are updated as sent/skipped,
    # - the part that actually sends alerts to slack
    # instead assert they're as expected.

    config = Config(
        config_dir=Config.DEFAULT_CONFIG_DIR,
        profiles_dir=None,
        project_dir=None,
        profile_target=warehouse_type,
        project_profile_target=warehouse_type,
        dbt_quoting=None,
        slack_webhook=None,
        slack_token=None,
        slack_channel_name=SLACK_CHANNEL_TEST_NOT_USED_NAME,
        timezone=None,
        env="dev",
        slack_group_alerts_by=GroupingType.BY_ALERT.value,
    )
    anonymous_tracking = MockAnonymousTracking(config)

    data_monitoring = DataMonitoringAlertsMock(
        config=config,
        tracking=anonymous_tracking,
        force_update_dbt_package=False,
        send_test_message_on_success=False,
        disable_samples=False,
        filter=None,
    )
    data_monitoring.slack_client = SlackWebClientMock(
        token=None, webhook=None, tracking=None
    )

    success = data_monitoring.run_alerts(
        days_back, dbt_full_refresh=False, dbt_vars=None
    )

    # general "code not broken" assertion:
    assert success

    # assertions about sent alerts strongly tied our dbt package e2e:
    if (
        warehouse_type not in NUM_ALERTS_E2E_GIVES.keys()
    ):  # for generating test data for the first time in a new warehouse
        print(
            f"NUM_ALERTS_E2E_GIVES['{warehouse_type}']={data_monitoring.sent_alert_count}"
        )
        print(
            f"TABLES_TO_UPDATE_IN_EDR_RUN_ON_E2E['{warehouse_type}']={list(data_monitoring.alerts_api.sent_alerts.keys())}"
        )
        print(
            f"EXPECTED_MESSAGE_HEADERS_COUNT['{warehouse_type}']={sorted(Counter([try_parse_header_text_from_slack_message_schema(x) for x in data_monitoring.slack_client.sent_messages[SLACK_CHANNEL_TEST_NOT_USED_NAME]]).items())}"
        )
        # this flow shouldn't be left alone in production so assert False to get some attention
        # not writing assert False  to help whoever reads the error message.
        assert warehouse_type in NUM_ALERTS_E2E_GIVES.keys()

    if data_monitoring.sent_alert_count != NUM_ALERTS_E2E_GIVES[warehouse_type]:
        print("looks like either a failure or a few runs mixing up together:")
        print(f"data_monitoring.sent_alert_count={data_monitoring.sent_alert_count}")
        print(
            f"NUM_ALERTS_E2E_GIVES[warehouse_type]={NUM_ALERTS_E2E_GIVES[warehouse_type]}"
        )

    # even if we do have a few CI runs spamming, we should have more than 0
    assert data_monitoring.sent_alert_count > 0

    # assertions to make sure counting alerts sent by the alert fetcher and marked as sent by the data_monitoring is the same
    assert (
        sum(len(x) for x in data_monitoring.alerts_api.sent_alerts.values())
        == data_monitoring.sent_alert_count
    )
    assert (
        len(
            data_monitoring.slack_client.sent_messages[SLACK_CHANNEL_TEST_NOT_USED_NAME]
        )
        == data_monitoring.sent_alert_count
    )

    assert sorted(list(data_monitoring.alerts_api.sent_alerts.keys())) == sorted(
        TABLES_TO_UPDATE_IN_EDR_RUN_ON_E2E[warehouse_type]
    )

    assert sorted(
        Counter(
            [
                try_parse_header_text_from_slack_message_schema(x)
                for x in data_monitoring.slack_client.sent_messages["test"]
            ]
        ).items()
    ) == sorted(EXPECTED_MESSAGE_HEADERS_COUNT[warehouse_type])


@pytest.fixture(scope="session")
def warehouse_type(pytestconfig):
    return pytestconfig.getoption("warehouse_type")
