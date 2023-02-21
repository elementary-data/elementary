from elementary.monitor.alerts.malformed import MalformedAlert
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.test import TestAlert
from elementary.monitor.api.alerts.alert_filters import (
    _filter_alerts_by_model,
    _filter_alerts_by_node_names,
    _filter_alerts_by_owner,
    _filter_alerts_by_tag,
    filter_alerts,
)
from elementary.monitor.data_monitoring.schema import SelectorFilterSchema


def initial_alerts():
    test_alerts = [
        TestAlert(
            id="1",
            alert_class_id="test_id_1",
            model_unique_id="elementary.model_id_1",
            test_unique_id="test_id_1",
            test_name="test_1",
            test_created_at="2022-10-10 10:10:10",
            tags='["one", "two"]',
            owners='["jeff", "john"]',
        ),
        TestAlert(
            id="2",
            alert_class_id="test_id_2",
            model_unique_id="elementary.model_id_1",
            test_unique_id="test_id_2",
            test_name="test_2",
            test_created_at="2022-10-10 09:10:10",
            tags='["three"]',
            owners='["jeff", "john"]',
        ),
        TestAlert(
            id="3",
            alert_class_id="test_id_3",
            model_unique_id="elementary.model_id_2",
            test_unique_id="test_id_3",
            test_name="test_3",
            test_created_at="2022-10-10 10:10:10",
            # invalid tag
            tags="one",
            owners='["john"]',
        ),
        TestAlert(
            id="4",
            alert_class_id="test_id_4",
            model_unique_id="elementary.model_id_2",
            test_unique_id="test_id_4",
            test_name="test_4",
            test_created_at="2022-10-10 09:10:10",
            tags='["three", "four"]',
            owners='["jeff"]',
        ),
    ]
    model_alerts = [
        ModelAlert(
            id="1",
            alert_class_id="elementary.model_id_1",
            model_unique_id="elementary.model_id_1",
            alias="modely",
            path="my/path",
            original_path="",
            materialization="table",
            message="",
            full_refresh=False,
            detected_at="2022-10-10 10:00:00",
            alert_suppression_interval=0,
            tags='["one", "two"]',
            owners='["jeff", "john"]',
        ),
        ModelAlert(
            id="2",
            alert_class_id="elementary.model_id_1",
            model_unique_id="elementary.model_id_1",
            alias="modely",
            path="my/path",
            original_path="",
            materialization="table",
            message="",
            full_refresh=False,
            detected_at="2022-10-10 09:00:00",
            alert_suppression_interval=3,
            tags='["three"]',
            owners='["john"]',
        ),
        ModelAlert(
            id="3",
            alert_class_id="elementary.model_id_2",
            model_unique_id="elementary.model_id_2",
            alias="model2",
            path="my/path2",
            original_path="",
            materialization="table",
            message="",
            full_refresh=False,
            detected_at="2022-10-10 08:00:00",
            alert_suppression_interval=1,
            tags='["three", "four"]',
            owners='["jeff"]',
        ),
    ]
    malformed_alerts = [
        MalformedAlert(
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
        ),
        MalformedAlert(
            id="2",
            data=dict(
                id="2",
                alert_class_id="elementary.model_id_1",
                model_unique_id="elementary.model_id_1",
                alias="modely",
                path="my/path",
                original_path="",
                materialization="table",
                message="",
                full_refresh=False,
                detected_at="2022-10-10 09:00:00",
                alert_suppression_interval=3,
                tags='["three"]',
                owners='["john"]',
            ),
        ),
    ]
    return test_alerts, model_alerts, malformed_alerts


def test_filter_alerts():
    test_alerts, model_alerts, malformed_alerts = initial_alerts()

    # Test that empty filter returns all of the alerts.
    filter = SelectorFilterSchema()
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    filter_malformed_alerts = filter_alerts(malformed_alerts, filter)
    assert len(filter_test_alerts) == len(test_alerts)
    assert len(filter_model_alerts) == len(model_alerts)
    assert len(filter_malformed_alerts) == len(malformed_alerts)

    # Test that passing no filter returns all of the alerts.
    filter_test_alerts = filter_alerts(test_alerts)
    filter_model_alerts = filter_alerts(model_alerts)
    filter_malformed_alerts = filter_alerts(malformed_alerts)
    assert len(filter_test_alerts) == len(test_alerts)
    assert len(filter_model_alerts) == len(model_alerts)
    assert len(filter_malformed_alerts) == len(malformed_alerts)

    # Test that filter with unsupported selector returns no alert
    filter = SelectorFilterSchema(last_invocation=True, selector="last_invocation")
    filter_test_alerts = filter_alerts(test_alerts, filter)
    filter_model_alerts = filter_alerts(model_alerts, filter)
    filter_malformed_alerts = filter_alerts(malformed_alerts, filter)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 0
    assert len(filter_malformed_alerts) == 0


def test_filter_alerts_by_tag():
    test_alerts, model_alerts, malformed_alerts = initial_alerts()

    filter = SelectorFilterSchema(tag="one")
    filter_test_alerts = _filter_alerts_by_tag(test_alerts, filter)
    filter_model_alerts = _filter_alerts_by_tag(model_alerts, filter)
    filter_malformed_alerts = _filter_alerts_by_tag(malformed_alerts, filter)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "1"
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "1"
    assert len(filter_malformed_alerts) == 1
    assert filter_malformed_alerts[0].id == "1"

    filter = SelectorFilterSchema(tag="three")
    filter_test_alerts = _filter_alerts_by_tag(test_alerts, filter)
    filter_model_alerts = _filter_alerts_by_tag(model_alerts, filter)
    filter_malformed_alerts = _filter_alerts_by_tag(malformed_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "2"
    assert filter_test_alerts[1].id == "4"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "2"
    assert filter_model_alerts[1].id == "3"
    assert len(filter_malformed_alerts) == 1
    assert filter_malformed_alerts[0].id == "2"

    filter = SelectorFilterSchema(tag="four")
    filter_test_alerts = _filter_alerts_by_tag(test_alerts, filter)
    filter_model_alerts = _filter_alerts_by_tag(model_alerts, filter)
    filter_malformed_alerts = _filter_alerts_by_tag(malformed_alerts, filter)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "4"
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "3"
    assert len(filter_malformed_alerts) == 0


def test_filter_alerts_by_owner():
    test_alerts, model_alerts, malformed_alerts = initial_alerts()

    filter = SelectorFilterSchema(owner="jeff")
    filter_test_alerts = _filter_alerts_by_owner(test_alerts, filter)
    filter_model_alerts = _filter_alerts_by_owner(model_alerts, filter)
    filter_malformed_alerts = _filter_alerts_by_owner(malformed_alerts, filter)
    assert len(filter_test_alerts) == 3
    assert filter_test_alerts[0].id == "1"
    assert filter_test_alerts[1].id == "2"
    assert filter_test_alerts[2].id == "4"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "1"
    assert filter_model_alerts[1].id == "3"
    assert len(filter_malformed_alerts) == 1
    assert filter_malformed_alerts[0].id == "1"

    filter = SelectorFilterSchema(owner="john")
    filter_test_alerts = _filter_alerts_by_owner(test_alerts, filter)
    filter_model_alerts = _filter_alerts_by_owner(model_alerts, filter)
    filter_malformed_alerts = _filter_alerts_by_owner(malformed_alerts, filter)
    assert len(filter_test_alerts) == 3
    assert filter_test_alerts[0].id == "1"
    assert filter_test_alerts[1].id == "2"
    assert filter_test_alerts[2].id == "3"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "1"
    assert filter_model_alerts[1].id == "2"
    assert len(filter_malformed_alerts) == 2
    assert filter_malformed_alerts[0].id == "1"
    assert filter_malformed_alerts[1].id == "2"


def test_filter_alerts_by_model():
    test_alerts, model_alerts, malformed_alerts = initial_alerts()

    filter = SelectorFilterSchema(model="model_id_1")
    filter_test_alerts = _filter_alerts_by_model(test_alerts, filter)
    filter_model_alerts = _filter_alerts_by_model(model_alerts, filter)
    filter_malformed_alerts = _filter_alerts_by_model(malformed_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "1"
    assert filter_test_alerts[1].id == "2"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "1"
    assert filter_model_alerts[1].id == "2"
    assert len(filter_malformed_alerts) == 2
    assert filter_malformed_alerts[0].id == "1"
    assert filter_malformed_alerts[1].id == "2"

    filter = SelectorFilterSchema(model="model_id_2")
    filter_test_alerts = _filter_alerts_by_model(test_alerts, filter)
    filter_model_alerts = _filter_alerts_by_model(model_alerts, filter)
    filter_malformed_alerts = _filter_alerts_by_model(malformed_alerts, filter)
    assert len(filter_test_alerts) == 2
    assert filter_test_alerts[0].id == "3"
    assert filter_test_alerts[1].id == "4"
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "3"
    assert len(filter_malformed_alerts) == 0


def test_filter_alerts_by_node_names():
    test_alerts, model_alerts, malformed_alerts = initial_alerts()

    filter = SelectorFilterSchema(node_names=["test_3", "model_id_1"])
    filter_test_alerts = _filter_alerts_by_node_names(test_alerts, filter)
    filter_model_alerts = _filter_alerts_by_node_names(model_alerts, filter)
    filter_malformed_alerts = _filter_alerts_by_node_names(malformed_alerts, filter)
    assert len(filter_test_alerts) == 1
    assert filter_test_alerts[0].id == "3"
    assert len(filter_model_alerts) == 2
    assert filter_model_alerts[0].id == "1"
    assert filter_model_alerts[1].id == "2"
    assert len(filter_malformed_alerts) == 1
    assert filter_malformed_alerts[0].id == "2"

    filter = SelectorFilterSchema(node_names=["model_id_2"])
    filter_test_alerts = _filter_alerts_by_node_names(test_alerts, filter)
    filter_model_alerts = _filter_alerts_by_node_names(model_alerts, filter)
    filter_malformed_alerts = _filter_alerts_by_node_names(malformed_alerts, filter)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 1
    assert filter_model_alerts[0].id == "3"
    assert len(filter_malformed_alerts) == 0

    filter = SelectorFilterSchema(node_names=["model_id_3"])
    filter_malformed_alerts = _filter_alerts_by_node_names(malformed_alerts, filter)
    filter_test_alerts = _filter_alerts_by_node_names(test_alerts, filter)
    filter_model_alerts = _filter_alerts_by_node_names(model_alerts, filter)
    assert len(filter_test_alerts) == 0
    assert len(filter_model_alerts) == 0
    assert len(filter_malformed_alerts) == 0

    filter = SelectorFilterSchema(node_names=["test_1"])
    filter_malformed_alerts = _filter_alerts_by_node_names(malformed_alerts, filter)
    assert len(filter_malformed_alerts) == 1
    assert filter_malformed_alerts[0].id == "1"
