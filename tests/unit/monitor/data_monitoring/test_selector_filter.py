from unittest import mock

import pytest
from parametrization import Parametrization

from elementary.monitor.data_monitoring.schema import ResourceType, Status
from elementary.monitor.data_monitoring.selector_filter import (
    InvalidSelectorError,
    SelectorFilter,
)
from tests.mocks.anonymous_tracking_mock import MockAnonymousTracking
from tests.mocks.dbt_runner_mock import MockDbtRunner


def test_parse_selector_with_user_dbt_runner(dbt_runner_mock, anonymous_tracking_mock):
    dbt_runner_mock.ls = mock.Mock(return_value=[])
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        tracking=anonymous_tracking_mock,
        user_dbt_runner=dbt_runner_mock,
        selector="mock:selector",
    )
    dbt_runner_mock.ls.assert_called_once()
    assert data_monitoring_filter_with_user_dbt_runner.get_filter().node_names == []
    assert data_monitoring_filter_with_user_dbt_runner.get_selector() == "mock:selector"

    dbt_runner_mock.ls = mock.Mock(return_value=["node_name_1", "node_name_2"])
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        tracking=anonymous_tracking_mock,
        user_dbt_runner=dbt_runner_mock,
        selector="mock:selector",
    )
    dbt_runner_mock.ls.assert_called_once()
    assert data_monitoring_filter_with_user_dbt_runner.get_filter().node_names == [
        "node_name_1",
        "node_name_2",
    ]
    assert data_monitoring_filter_with_user_dbt_runner.get_selector() == "mock:selector"


def test_parse_selector_without_user_dbt_runner(anonymous_tracking_mock):
    # tag selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        tracking=anonymous_tracking_mock,
        selector="tag:mock_tag",
    )
    assert data_monitoring_filter_with_user_dbt_runner.get_filter().tag == "mock_tag"
    assert data_monitoring_filter_with_user_dbt_runner.get_selector() == "tag:mock_tag"

    # owner selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        tracking=anonymous_tracking_mock,
        selector="config.meta.owner:mock_owner",
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().owner == "mock_owner"
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_selector()
        == "config.meta.owner:mock_owner"
    )

    # model selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        tracking=anonymous_tracking_mock,
        selector="model:mock_model",
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().model == "mock_model"
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_selector() == "model:mock_model"
    )

    # status selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        tracking=anonymous_tracking_mock,
        selector="statuses:fail,error",
    )
    assert data_monitoring_filter_with_user_dbt_runner.get_filter().statuses == [
        Status.FAIL,
        Status.ERROR,
    ]
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_selector()
        == "statuses:fail,error"
    )

    # resource type selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        tracking=anonymous_tracking_mock,
        selector="resource_types:model",
    )
    assert data_monitoring_filter_with_user_dbt_runner.get_filter().resource_types == [
        ResourceType.MODEL
    ]
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_selector()
        == "resource_types:model"
    )

    # invocation_id selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        tracking=anonymous_tracking_mock,
        selector="invocation_id:mock_invocation_id",
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().invocation_id
        == "mock_invocation_id"
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_selector()
        == "invocation_id:mock_invocation_id"
    )

    # invocation_time selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        tracking=anonymous_tracking_mock,
        selector="invocation_time:2023-02-08 10:00:00",
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().invocation_time
        is not None
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_selector()
        == "invocation_time:2023-02-08 10:00:00"
    )

    # invalid_invocation_time selector
    with pytest.raises(ValueError):
        data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
            tracking=anonymous_tracking_mock,
            selector="invocation_time:2023-32-32",
        )

    # last_invocation selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        tracking=anonymous_tracking_mock,
        selector="last_invocation",
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().last_invocation is True
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_selector() == "last_invocation"
    )

    # unsupported selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        tracking=anonymous_tracking_mock,
        selector="blabla:blublu",
    )
    dbt_runner_get_filter = data_monitoring_filter_with_user_dbt_runner.get_filter()
    assert dbt_runner_get_filter.tag is None
    assert dbt_runner_get_filter.owner is None
    assert dbt_runner_get_filter.model is None
    assert dbt_runner_get_filter.last_invocation is False
    assert dbt_runner_get_filter.invocation_id is None
    assert dbt_runner_get_filter.invocation_time is None
    assert dbt_runner_get_filter.node_names is None
    assert dbt_runner_get_filter.resource_types is None
    assert dbt_runner_get_filter.statuses == []
    assert data_monitoring_filter_with_user_dbt_runner.get_selector() == "blabla:blublu"


@Parametrization.autodetect_parameters()
@Parametrization.case(name="None", selector=None, should_raise=False)
@Parametrization.case(
    name="report filter1",
    selector="invocation_id:mock_invocation_id",
    should_raise=False,
)
@Parametrization.case(
    name="report filter2",
    selector="invocation_time:mock_invocation_time",
    should_raise=False,
)
@Parametrization.case(
    name="report filter3", selector="last_invocation", should_raise=False
)
@Parametrization.case(name="alerts filter1", selector="model=blabla", should_raise=True)
@Parametrization.case(name="alerts filter2", selector="tag=blabla", should_raise=True)
@Parametrization.case(
    name="alerts filter3", selector="statuses=blabla", should_raise=True
)
def test_validate_report_selector(selector, should_raise):
    if should_raise:
        with pytest.raises(InvalidSelectorError):
            SelectorFilter.validate_report_selector(selector)
    else:
        SelectorFilter.validate_report_selector(selector)


@Parametrization.autodetect_parameters()
@Parametrization.case(name="None", selector=None, should_raise=False)
@Parametrization.case(
    name="report filter1",
    selector="invocation_id:mock_invocation_id",
    should_raise=True,
)
@Parametrization.case(
    name="report filter2",
    selector="invocation_time:mock_invocation_time",
    should_raise=True,
)
@Parametrization.case(
    name="report filter3", selector="last_invocation", should_raise=True
)
@Parametrization.case(
    name="alerts filter1", selector="model=blabla", should_raise=False
)
@Parametrization.case(name="alerts filter2", selector="tag=blabla", should_raise=False)
@Parametrization.case(
    name="alerts filter3", selector="statuses=blabla", should_raise=False
)
def test_validate_alerts_selector(selector, should_raise):
    if should_raise:
        with pytest.raises(InvalidSelectorError):
            SelectorFilter.validate_alert_selector(selector)
    else:
        SelectorFilter.validate_alert_selector(selector)


@pytest.fixture
def dbt_runner_mock() -> MockDbtRunner:
    return MockDbtRunner()


@pytest.fixture
def anonymous_tracking_mock() -> MockAnonymousTracking:
    return MockAnonymousTracking()
