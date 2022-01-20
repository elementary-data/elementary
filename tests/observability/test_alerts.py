import pytest
import json
from datetime import datetime

from utils.time import convert_utc_time_to_local_time

from observability.alerts import Alert, SchemaChangeAlert


def test_alert_create_alert_from_row():
    row = ['123', datetime.now(), 'db', 'sc', 't1', 'c1', 'schema_change', 'column_added', 'Column was added']
    alert = Alert.create_alert_from_row(row)
    assert isinstance(alert, SchemaChangeAlert)
    assert alert.id == row[0]


def test_schema_change_alert_to_slack_message():
    alert_time = datetime.now()
    alert = SchemaChangeAlert('123', 'db', 'sc', 't1', alert_time, 'column_added', 'Column was added')
    schema_change_slack_msg_str = json.dumps(alert.to_slack_message()).lower()
    assert alert.table_name.lower() in schema_change_slack_msg_str
    assert alert.change_type.lower() in schema_change_slack_msg_str
    assert alert.description.lower() in schema_change_slack_msg_str
    local_time = convert_utc_time_to_local_time(alert_time)
    assert local_time.strftime('%Y-%m-%d %H:%M:%S') in schema_change_slack_msg_str
