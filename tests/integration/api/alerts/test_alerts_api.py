from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.config.config import Config
from elementary.monitor.alerts.alerts import Alerts, AlertsQueryResult, AlertType
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.test import DbtTestAlert, ElementaryTestAlert, TestAlert
from elementary.monitor.api.alerts.alerts import AlertsAPI


class MockAlertsAPI(AlertsAPI):
    def _query_pending_test_alerts(self, **kwargs):
        PENDDING_TEST_ALERTS_MOCK_DATA = [
            # Alert withting suppression interval
            dict(
                id="alert_id_1",
                unique_id="test_id_1",
                test_unique_id="test_id_1",
                model_unique_id="model_id_1",
                detected_at="2022-10-10 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                table_name="table",
                column_name="column",
                test_type="dbt_test",
                test_sub_type="generic",
                test_results_description="a mock alert",
                owners='["jeff"]',
                tags='["best_test"]',
                test_results_query="select * from table",
                test_rows_sample="[]",
                other=None,
                test_name="test_1",
                test_short_name="short",
                test_params="{}",
                severity="ERROR",
                test_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
                model_meta="{}",
                status="fail",
                suppression_status="pending",
                sent_at=None,
                test_created_at="2022-10-09 10:10:10",
            ),
            # Alert after suppression interval
            dict(
                id="alert_id_2",
                unique_id="test_id_1",
                test_unique_id="test_id_1",
                model_unique_id="model_id_1",
                detected_at="2022-10-11 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                table_name="table",
                column_name="column",
                test_type="dbt_test",
                test_sub_type="generic",
                test_results_description="a mock alert",
                owners='["jeff"]',
                tags='["best_test"]',
                test_results_query="select * from table",
                test_rows_sample="[]",
                other=None,
                test_name="test_1",
                test_short_name="short",
                test_params="{}",
                severity="ERROR",
                test_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
                model_meta="{}",
                status="fail",
                suppression_status="pending",
                sent_at=None,
                test_created_at="2022-10-09 10:10:10",
            ),
            # Alert without suppression interval
            dict(
                id="alert_id_3",
                unique_id="test_id_2",
                test_unique_id="test_id_2",
                model_unique_id="model_id_2",
                detected_at="2022-10-10 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                table_name="table",
                column_name="column",
                test_type="anomaly_detection",
                test_sub_type="row_count",
                test_results_description="a mock alert",
                owners='["jeff"]',
                tags='["best_test"]',
                test_results_query="select * from table",
                test_rows_sample="[]",
                other=None,
                test_name="test_2",
                test_short_name="shorter",
                test_params="{}",
                severity="ERROR",
                test_meta='{ "alerts_config": {"alert_suppression_interval": 0} }',
                model_meta="{}",
                status="fail",
                suppression_status="pending",
                sent_at=None,
                test_created_at="2022-10-09 10:10:10",
            ),
            # First occurrence alert with suppression interval
            dict(
                id="alert_id_4",
                unique_id="test_id_3",
                test_unique_id="test_id_3",
                model_unique_id="model_id_2",
                detected_at="2022-10-10 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                table_name="table",
                column_name="column",
                test_type="anomaly_detection",
                test_sub_type="row_count",
                test_results_description="a mock alert",
                owners='["jeff"]',
                tags='["best_test"]',
                test_results_query="select * from table",
                test_rows_sample="[]",
                other=None,
                test_name="test_2",
                test_short_name="shorter",
                test_params="{}",
                severity="ERROR",
                test_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
                model_meta="{}",
                status="fail",
                suppression_status="pending",
                sent_at=None,
                test_created_at="2022-10-09 10:10:10",
            ),
        ]

        pending_test_alerts = [
            TestAlert.create_test_alert_from_dict(**pending_alert)
            for pending_alert in PENDDING_TEST_ALERTS_MOCK_DATA
        ]
        return pending_test_alerts

    def _query_pending_model_alerts(self, **kwargs):
        PENDDING_MODEL_ALERTS_MOCK_DATA = [
            # Alert withting suppression interval
            dict(
                id="alert_id_1",
                unique_id="model_id_1",
                alias="model",
                path="",
                original_path="",
                materialization="table",
                detected_at="2022-10-10 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                full_refresh=False,
                message="",
                owners="[]",
                tags="[]",
                model_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
                suppression_status="pending",
                sent_at=None,
                status="error",
            ),
            # Alert after suppression interval
            dict(
                id="alert_id_2",
                unique_id="model_id_1",
                alias="model",
                path="",
                original_path="",
                materialization="table",
                detected_at="2022-10-11 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                full_refresh=False,
                message="",
                owners="[]",
                tags="[]",
                model_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
                suppression_status="pending",
                sent_at=None,
                status="error",
            ),
            # Alert without suppression interval
            dict(
                id="alert_id_3",
                unique_id="model_id_2",
                alias="model",
                path="",
                original_path="",
                materialization="table",
                detected_at="2022-10-10 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                full_refresh=False,
                message="",
                owners="[]",
                tags="[]",
                model_meta='{ "alerts_config": {"alert_suppression_interval": 0} }',
                suppression_status="pending",
                sent_at=None,
                status="error",
            ),
            # First occurrence alert with suppression interval
            dict(
                id="alert_id_4",
                unique_id="model_id_3",
                alias="model",
                path="",
                original_path="",
                materialization="table",
                detected_at="2022-10-10 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                full_refresh=False,
                message="",
                owners="[]",
                tags="[]",
                model_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
                suppression_status="pending",
                sent_at=None,
                status="error",
            ),
        ]

        pending_model_alerts = [
            ModelAlert(**pending_alert)
            for pending_alert in PENDDING_MODEL_ALERTS_MOCK_DATA
        ]
        return pending_model_alerts

    def _query_pending_source_freshness_alerts(self, **kwargs):
        PENDDING_SOURVE_FRESHNESS_ALERTS_MOCK_DATA = [
            # Alert withting suppression interval
            dict(
                id="alert_id_1",
                unique_id="source_id_1",
                detected_at="2022-10-11 10:00:00",
                snapshotted_at="2022-10-11 10:00:00",
                max_loaded_at="2022-10-11 10:00:00",
                max_loaded_at_time_ago_in_s="123123",
                database_name="test_db",
                schema_name="test_scehma",
                source_name="source_1",
                identifier="identifier",
                freshness_error_after="10",
                freshness_warn_after="10",
                freshness_filter="",
                status="fail",
                owners="[]",
                path="",
                error="",
                tags="[]",
                model_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
                suppression_status="pending",
                sent_at=None,
            ),
            # Alert after suppression interval
            dict(
                id="alert_id_2",
                unique_id="model_id_1",
                alias="model",
                path="",
                original_path="",
                materialization="table",
                detected_at="2022-10-11 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                full_refresh=False,
                message="",
                owners="[]",
                tags="[]",
                model_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
                suppression_status="pending",
                sent_at=None,
                status="error",
            ),
            # Alert without suppression interval
            dict(
                id="alert_id_3",
                unique_id="model_id_2",
                alias="model",
                path="",
                original_path="",
                materialization="table",
                detected_at="2022-10-10 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                full_refresh=False,
                message="",
                owners="[]",
                tags="[]",
                model_meta='{ "alerts_config": {"alert_suppression_interval": 0} }',
                suppression_status="pending",
                sent_at=None,
                status="error",
            ),
            # First occurrence alert with suppression interval
            dict(
                id="alert_id_4",
                unique_id="model_id_3",
                alias="model",
                path="",
                original_path="",
                materialization="table",
                detected_at="2022-10-10 10:00:00",
                database_name="test_db",
                schema_name="test_schema",
                full_refresh=False,
                message="",
                owners="[]",
                tags="[]",
                model_meta='{ "alerts_config": {"alert_suppression_interval": 1} }',
                suppression_status="pending",
                sent_at=None,
                status="error",
            ),
        ]
        pass

    def _query_last_test_alert_times(self, **kwargs):
        pass

    def _query_last_model_alert_times(self, **kwargs):
        pass

    def _query_last_source_freshness_alert_times(self, **kwargs):
        pass


def test_bla():
    project_dir = "proj_dir"
    profiles_dir = "prof_dir"
    dbt_runner = DbtRunner(project_dir=project_dir, profiles_dir=profiles_dir)
    config = Config()
    api = MockAlertsAPI(
        dbt_runner=dbt_runner, config=config, elementary_database_and_schema="test.test"
    )
    api._query_pending_test_alerts()
    api._query_pending_model_alerts()
