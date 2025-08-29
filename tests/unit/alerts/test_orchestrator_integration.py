import pytest

from elementary.messages.blocks import Icon
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.utils.orchestrator_link import (
    OrchestratorLinkData,
    create_job_link,
    create_orchestrator_link,
)


class TestOrchestratorLinkCreation:
    """Test orchestrator link creation functionality."""

    def test_create_orchestrator_link_with_valid_data(self):
        orchestrator_info = {
            "job_name": "nightly_load",
            "run_id": "12345",
            "orchestrator": "airflow",
            "run_url": "https://airflow.example.com/run/12345",
        }

        link = create_orchestrator_link(orchestrator_info)

        assert link is not None
        assert link.text == "View in Airflow"
        assert link.url == "https://airflow.example.com/run/12345"
        assert link.orchestrator == "airflow"
        assert link.icon == Icon.INFO

    def test_create_orchestrator_link_formats_orchestrator_names(self):
        test_cases = [
            ("airflow", "View in Airflow"),
            ("dbt_cloud", "View in Dbt Cloud"),
            ("github_actions", "View in Github Actions"),
            ("custom_orchestrator", "View in Custom Orchestrator"),
        ]

        for orchestrator, expected_text in test_cases:
            orchestrator_info = {
                "orchestrator": orchestrator,
                "run_url": "https://example.com/run/123",  # noqa: E231
            }

            link = create_orchestrator_link(orchestrator_info)

            assert link is not None
            assert link.text == expected_text
            assert link.orchestrator == orchestrator

    def test_create_orchestrator_link_without_url_returns_none(self):
        orchestrator_info = {
            "job_name": "nightly_load",
            "orchestrator": "airflow"
            # No run_url
        }

        link = create_orchestrator_link(orchestrator_info)
        assert link is None

    def test_create_orchestrator_link_with_empty_info_returns_none(self):
        link = create_orchestrator_link({})
        assert link is None

    def test_create_job_link_with_valid_data(self):
        orchestrator_info = {
            "job_name": "nightly_load",
            "orchestrator": "airflow",
            "job_url": "https://airflow.example.com/job/nightly_load",
        }

        link = create_job_link(orchestrator_info)

        assert link is not None
        assert link.text == "nightly_load in Airflow"
        assert link.url == "https://airflow.example.com/job/nightly_load"
        assert link.orchestrator == "airflow"
        assert link.icon == Icon.GEAR

    def test_create_job_link_without_url_returns_none(self):
        orchestrator_info = {
            "job_name": "nightly_load",
            "orchestrator": "airflow"
            # No job_url
        }

        link = create_job_link(orchestrator_info)
        assert link is None


class TestAlertModelOrchestratorInfo:
    """Test AlertModel orchestrator_info property across all alert types."""

    def test_test_alert_orchestrator_info_with_complete_data(self):
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
            job_name="nightly_load",
            job_run_id="12345",
            orchestrator="airflow",
            job_url="https://airflow.example.com/job/nightly_load",
            job_run_url="https://airflow.example.com/run/12345",
        )

        info = alert.orchestrator_info
        assert info is not None
        assert info["job_name"] == "nightly_load"
        assert info["run_id"] == "12345"
        assert info["orchestrator"] == "airflow"
        assert info["job_url"] == "https://airflow.example.com/job/nightly_load"
        assert info["run_url"] == "https://airflow.example.com/run/12345"

    def test_test_alert_orchestrator_info_with_minimal_data(self):
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
            job_name="test_job"
            # Only job_name provided
        )

        info = alert.orchestrator_info
        assert info is not None
        assert info["job_name"] == "test_job"
        assert len(info) == 1

    def test_test_alert_orchestrator_info_with_no_data_returns_none(self):
        alert = TestAlertModel(
            id="test_id",
            test_unique_id="test_unique_id",
            elementary_unique_id="elementary_unique_id",
            test_name="test_name",
            severity="error",
            test_type="dbt_test",
            test_sub_type="generic",
            test_short_name="test_short_name",
            alert_class_id="test_alert_class_id"
            # No orchestrator fields
        )

        info = alert.orchestrator_info
        assert info is None

    def test_model_alert_orchestrator_info(self):
        alert = ModelAlertModel(
            id="model_alert_id",
            alias="test_model",
            path="/models/test_model.sql",
            original_path="models/test_model.sql",
            materialization="table",
            full_refresh=False,
            alert_class_id="model_alert_class",
            job_name="nightly_build",
            job_run_id="67890",
            orchestrator="dbt_cloud",
            job_run_url="https://cloud.getdbt.com/run/67890",
        )

        info = alert.orchestrator_info
        assert info is not None
        assert info["job_name"] == "nightly_build"
        assert info["run_id"] == "67890"
        assert info["orchestrator"] == "dbt_cloud"
        assert info["run_url"] == "https://cloud.getdbt.com/run/67890"

    def test_source_freshness_alert_orchestrator_info(self):
        alert = SourceFreshnessAlertModel(
            id="source_alert_id",
            source_name="test_source",
            identifier="test_table",
            original_status="error",
            path="sources.yml",
            error="Freshness check failed",
            alert_class_id="source_alert_class",
            source_freshness_execution_id="exec_123",
            job_name="freshness_check",
            orchestrator="airflow",
            job_run_url="https://airflow.example.com/run/111",
        )

        info = alert.orchestrator_info
        assert info is not None
        assert info["job_name"] == "freshness_check"
        assert info["orchestrator"] == "airflow"
        assert info["run_url"] == "https://airflow.example.com/run/111"


class TestOrchestratorInfoEdgeCases:
    """Test edge cases and error handling for orchestrator integration."""

    def test_orchestrator_info_with_empty_strings(self):
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
            job_name="",  # Empty string
            orchestrator="",  # Empty string
            job_run_url="",  # Empty string
        )

        info = alert.orchestrator_info
        assert info is None

    def test_orchestrator_info_filters_none_values(self):
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
            job_name="valid_job",
            orchestrator=None,  # None value
            job_run_url=None,  # None value
        )

        info = alert.orchestrator_info
        assert info is not None
        assert "job_name" in info
        assert "orchestrator" not in info
        assert "run_url" not in info
        assert len(info) == 1

    def test_orchestrator_info_with_only_run_id(self):
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
            job_run_id="12345",  # Only run_id provided
        )

        info = alert.orchestrator_info
        assert info is not None
        assert info["run_id"] == "12345"
        assert len(info) == 1

    def test_orchestrator_info_with_only_orchestrator(self):
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
            orchestrator="dbt_cloud",  # Only orchestrator provided
        )

        info = alert.orchestrator_info
        assert info is not None
        assert info["orchestrator"] == "dbt_cloud"
        assert len(info) == 1


class TestOrchestratorLinkDataModel:
    """Test OrchestratorLinkData model validation."""

    def test_orchestrator_link_data_creation(self):
        link = OrchestratorLinkData(
            url="https://airflow.example.com/run/123",
            text="View in Airflow",
            orchestrator="airflow",
            icon=Icon.INFO,
        )

        assert link.url == "https://airflow.example.com/run/123"
        assert link.text == "View in Airflow"
        assert link.orchestrator == "airflow"
        assert link.icon == Icon.INFO

    def test_orchestrator_link_data_without_icon(self):
        link = OrchestratorLinkData(
            url="https://airflow.example.com/run/123",
            text="View in Airflow",
            orchestrator="airflow"
            # icon is optional
        )

        assert link.icon is None

    def test_orchestrator_link_data_required_fields(self):
        # Should raise validation error if required fields are missing
        with pytest.raises((TypeError, ValueError)):
            OrchestratorLinkData(
                # Missing required fields
                icon=Icon.INFO
            )
