import json

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.config.config import Config
from elementary.monitor.alerts.alerts import AlertsQueryResult
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.test import TestAlert
from elementary.monitor.api.alerts.alerts import AlertsAPI


def _initial_alerts_api_client():
    project_dir = "proj_dir"
    profiles_dir = "prof_dir"
    dbt_runner = DbtRunner(project_dir=project_dir, profiles_dir=profiles_dir)
    config = Config()
    return AlertsAPI(
        dbt_runner=dbt_runner, config=config, elementary_database_and_schema="test.test"
    )


def test_get_latest_alerts():
    alerts_api = _initial_alerts_api_client()
    alert_1 = TestAlert(
        id="1",
        model_unique_id="model_id_1",
        test_unique_id="test_id_1",
        test_created_at="2022-10-10 10:10:10",
        detected_at="2022-10-10 10:00:00",
    )
    alert_2 = TestAlert(
        id="2",
        model_unique_id="model_id_1",
        test_unique_id="test_id_1",
        test_created_at="2022-10-10 09:10:10",
        detected_at="2022-10-10 09:00:00",
    )
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
    )
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
    )
    alerts = AlertsQueryResult(
        alerts=[alert_1, alert_2, alert_3, alert_4], malformed_alerts=[]
    )
    latest_alerts = alerts_api._get_latest_alerts(alerts)
    assert json.dumps(latest_alerts, sort_keys=True) == json.dumps(
        [alert_1.id, alert_3.id], sort_keys=True
    )
