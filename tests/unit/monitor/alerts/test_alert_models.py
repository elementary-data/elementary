import pytest

from elementary.monitor.alerts.alert import AlertModel
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel


def _make_test_alert(
    test_sub_type: str = "generic", test_short_name: str = "my_test"
) -> TestAlertModel:
    return TestAlertModel(
        id="id",
        test_unique_id="tuid",
        elementary_unique_id="euid",
        test_name="test_name",
        severity="error",
        test_type="dbt_test",
        test_sub_type=test_sub_type,
        test_short_name=test_short_name,
        alert_class_id="acid",
    )


def _make_model_alert(
    alias: str = "my_model", materialization: str = "table"
) -> ModelAlertModel:
    return ModelAlertModel(
        id="id",
        alias=alias,
        path="models/m.sql",
        original_path="models/m.sql",
        full_refresh=False,
        alert_class_id="acid",
        materialization=materialization,
    )


def _make_source_freshness_alert(
    source_name: str = "my_source", identifier: str = "my_table"
) -> SourceFreshnessAlertModel:
    return SourceFreshnessAlertModel(
        id="id",
        source_name=source_name,
        identifier=identifier,
        original_status="fail",
        path="models/src.yml",
        error=None,
        alert_class_id="acid",
        source_freshness_execution_id="sfeid",
    )


class TestAlertModelContract:
    def test_base_asset_type_raises(self) -> None:
        alert = AlertModel(id="id", alert_class_id="acid")
        with pytest.raises(NotImplementedError):
            _ = alert.asset_type

    def test_base_concise_name_raises(self) -> None:
        alert = AlertModel(id="id", alert_class_id="acid")
        with pytest.raises(NotImplementedError):
            _ = alert.concise_name


class TestTestAlertModel:
    def test_asset_type_is_test(self) -> None:
        assert _make_test_alert().asset_type == "test"

    def test_concise_name_generic(self) -> None:
        alert = _make_test_alert(test_sub_type="generic", test_short_name="my_test")
        assert alert.concise_name == "my_test"

    def test_concise_name_non_generic_includes_sub_type(self) -> None:
        alert = _make_test_alert(test_sub_type="row_count", test_short_name="my_test")
        assert alert.concise_name == "my_test - Row Count"


class TestModelAlertModel:
    def test_asset_type_model_when_not_snapshot(self) -> None:
        assert _make_model_alert(materialization="table").asset_type == "model"

    def test_asset_type_snapshot_when_snapshot(self) -> None:
        assert _make_model_alert(materialization="snapshot").asset_type == "snapshot"

    def test_concise_name_is_alias(self) -> None:
        alert = _make_model_alert(alias="my_model")
        assert alert.concise_name == "my_model"


class TestSourceFreshnessAlertModel:
    def test_asset_type_is_source(self) -> None:
        assert _make_source_freshness_alert().asset_type == "source"

    def test_concise_name_is_source_dot_identifier(self) -> None:
        alert = _make_source_freshness_alert(source_name="raw", identifier="orders")
        assert alert.concise_name == "raw.orders"
