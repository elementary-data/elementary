import json
from datetime import datetime, timedelta

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.config.config import Config
from elementary.monitor.alerts.alerts import AlertsQueryResult
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.test import TestAlert
from elementary.monitor.api.alerts.alerts import AlertsAPI
from elementary.utils.time import DATETIME_FORMAT


def _initial_alerts_api_client():
    project_dir = "proj_dir"
    profiles_dir = "prof_dir"
    dbt_runner = DbtRunner(project_dir=project_dir, profiles_dir=profiles_dir)
    config = Config()
    return AlertsAPI(
        dbt_runner=dbt_runner, config=config, elementary_database_and_schema="test.test"
    )


def test_get_suppressed_alerts():
    alerts_api = _initial_alerts_api_client()
    current_time = datetime.utcnow()
    alert_last_sent_time = (current_time - timedelta(hours=1.5)).strftime(
        DATETIME_FORMAT
    )
    last_test_alert_sent_times = dict(
        test_id_1=alert_last_sent_time,
        test_id_2=alert_last_sent_time,
    )
    last_model_alert_sent_times = dict(
        model_id_1=alert_last_sent_time,
        model_id_2=alert_last_sent_time,
    )
    # Test alert with interval that passed the suppression time
    alert_1 = TestAlert(
        id="1",
        unique_id="test_id_1",
        model_unique_id="model_id_1",
        test_unique_id="test_id_1",
        test_created_at="2022-10-10 10:10:10",
        detected_at="2022-10-10 10:00:00",
        alert_suppression_interval=1,
    )
    # Test alert with interval that hasn't passed the suppression time
    alert_2 = TestAlert(
        id="2",
        unique_id="test_id_2",
        model_unique_id="model_id_1",
        test_unique_id="test_id_2",
        test_created_at="2022-10-10 09:10:10",
        detected_at="2022-10-10 09:00:00",
        alert_suppression_interval=2,
    )
    # Model alert with interval that passed the suppression time
    alert_3 = ModelAlert(
        id="3",
        unique_id="model_id_1",
        alias="modely",
        path="my/path",
        original_path="",
        materialization="table",
        message="",
        full_refresh=False,
        detected_at="2022-10-10 10:00:00",
        alert_suppression_interval=0,
    )
    # Model alert with interval that hasn't passed the suppression time
    alert_4 = ModelAlert(
        id="4",
        unique_id="model_id_1",
        alias="modely",
        path="my/path",
        original_path="",
        materialization="table",
        message="",
        full_refresh=False,
        detected_at="2022-10-10 09:00:00",
        alert_suppression_interval=3,
    )

    # Model alert first appearence
    alert_5 = ModelAlert(
        id="5",
        unique_id="model_id_2",
        alias="model2",
        path="my/path2",
        original_path="",
        materialization="table",
        message="",
        full_refresh=False,
        detected_at="2022-10-10 08:00:00",
        alert_suppression_interval=1,
    )
    test_alerts = AlertsQueryResult(alerts=[alert_1, alert_2], malformed_alerts=[])
    model_alerts = AlertsQueryResult(
        alerts=[alert_3, alert_4, alert_5], malformed_alerts=[]
    )
    suppressed_test_alerts = alerts_api._get_suppressed_alerts(
        test_alerts, last_test_alert_sent_times
    )
    suppressed_model_alerts = alerts_api._get_suppressed_alerts(
        model_alerts, last_model_alert_sent_times
    )
    assert json.dumps(suppressed_test_alerts, sort_keys=True) == json.dumps(
        [alert_2.id], sort_keys=True
    )
    assert json.dumps(suppressed_model_alerts, sort_keys=True) == json.dumps(
        [alert_4.id], sort_keys=True
    )
