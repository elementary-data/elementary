"""Simplified tests for orchestrator integration in alert messages."""

from unittest.mock import Mock

from elementary.messages.formats.block_kit import format_block_kit
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from tests.unit.alerts.alert_messages.test_alert_utils import get_alert_message_body


class TestOrchestratorMessageIntegration:
    """Test orchestrator integration in alert messages using actual message rendering."""

    def test_alert_message_includes_orchestrator_job_info(self):
        """Test that alerts with orchestrator data include job information."""
        alert = TestAlertModel(
            id="test_id",
            test_unique_id="test_unique_id",
            elementary_unique_id="elementary_unique_id",
            test_name="test_name",
            severity="error",
            test_type="dbt_test",
            test_sub_type="generic",
            test_short_name="test_short_name",
            alert_class_id="test_alert_class_id",
            status="fail",
            job_name="nightly_load",
            orchestrator="airflow",
        )

        # Mock get_report_link to avoid dependency
        alert.get_report_link = Mock(return_value=None)

        # Generate message and render to Slack format
        message_body = get_alert_message_body(alert)
        slack_message = format_block_kit(message_body)

        # Convert to JSON string for easy text search
        slack_json = str(slack_message)

        # Should contain job information
        assert "nightly_load" in slack_json
        assert "airflow" in slack_json

    def test_alert_message_includes_orchestrator_link(self):
        """Test that alerts with run URL include orchestrator links."""
        alert = TestAlertModel(
            id="test_id",
            test_unique_id="test_unique_id",
            elementary_unique_id="elementary_unique_id",
            test_name="test_name",
            severity="error",
            test_type="dbt_test",
            test_sub_type="generic",
            test_short_name="test_short_name",
            alert_class_id="test_alert_class_id",
            status="fail",
            job_name="nightly_load",
            orchestrator="dbt_cloud",
            job_run_url="https://cloud.getdbt.com/run/12345",
        )

        alert.get_report_link = Mock(return_value=None)

        message_body = get_alert_message_body(alert)
        slack_message = format_block_kit(message_body)
        slack_json = str(slack_message)

        # Should contain orchestrator link
        assert "cloud.getdbt.com/run/12345" in slack_json

    def test_alert_without_orchestrator_data_works_normally(self):
        """Test that alerts without orchestrator data work as before."""
        alert = TestAlertModel(
            id="test_id",
            test_unique_id="test_unique_id",
            elementary_unique_id="elementary_unique_id",
            test_name="test_name",
            severity="error",
            test_type="dbt_test",
            test_sub_type="generic",
            test_short_name="test_short_name",
            alert_class_id="test_alert_class_id",
            status="fail"
            # No orchestrator fields
        )

        alert.get_report_link = Mock(return_value=None)

        # Should not fail and should generate a valid message
        message_body = get_alert_message_body(alert)
        slack_message = format_block_kit(message_body)

        # Should be a valid Slack message structure (it's a Pydantic model, not a dict)
        assert slack_message is not None
        assert hasattr(slack_message, "attachments") or hasattr(slack_message, "blocks")

    def test_model_alert_with_orchestrator_data(self):
        """Test ModelAlertModel with orchestrator integration."""
        alert = ModelAlertModel(
            id="model_alert_id",
            alias="test_model",
            path="/models/test_model.sql",
            original_path="models/test_model.sql",
            materialization="table",
            full_refresh=False,
            alert_class_id="model_alert_class",
            status="error",
            job_name="nightly_build",
            orchestrator="github_actions",
            job_run_url="https://github.com/org/repo/actions/runs/123",
        )

        alert.get_report_link = Mock(return_value=None)

        message_body = get_alert_message_body(alert)
        slack_message = format_block_kit(message_body)
        slack_json = str(slack_message)

        # Should contain model orchestrator info
        assert "nightly_build" in slack_json
        assert "github_actions" in slack_json

    def test_orchestrator_info_property_integration(self):
        """Test that the orchestrator_info property works correctly."""
        alert = TestAlertModel(
            id="test_id",
            test_unique_id="test_unique_id",
            elementary_unique_id="elementary_unique_id",
            test_name="test_name",
            severity="error",
            test_type="dbt_test",
            test_sub_type="generic",
            test_short_name="test_short_name",
            alert_class_id="test_alert_class_id",
            job_name="integration_test",
            job_run_id="run_123",
            orchestrator="airflow",
            job_url="https://airflow.example.com/job/integration_test",
            job_run_url="https://airflow.example.com/run/123",
        )

        # Test orchestrator_info property
        orchestrator_info = alert.orchestrator_info

        assert orchestrator_info is not None
        assert orchestrator_info.job_name == "integration_test"
        assert orchestrator_info.run_id == "run_123"
        assert orchestrator_info.orchestrator == "airflow"
        assert (
            orchestrator_info.job_url
            == "https://airflow.example.com/job/integration_test"
        )
        assert orchestrator_info.run_url == "https://airflow.example.com/run/123"

        # Test message includes this data
        alert.get_report_link = Mock(return_value=None)

        message_body = get_alert_message_body(alert)
        slack_message = format_block_kit(message_body)
        slack_json = str(slack_message)

        assert "integration_test" in slack_json
        assert "airflow" in slack_json
        assert "airflow.example.com/run/123" in slack_json
