from unittest import mock

import pytest

from elementary.monitor.data_monitoring.selector_filter import SelectorFilter
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
    assert data_monitoring_filter_with_user_dbt_runner.get_filter().tag is None
    assert data_monitoring_filter_with_user_dbt_runner.get_filter().owner is None
    assert data_monitoring_filter_with_user_dbt_runner.get_filter().model is None
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().last_invocation
        is False
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().invocation_id is None
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().invocation_time is None
    )
    assert data_monitoring_filter_with_user_dbt_runner.get_filter().node_names is None


@pytest.fixture
def dbt_runner_mock() -> MockDbtRunner:
    return MockDbtRunner()


@pytest.fixture
def anonymous_tracking_mock() -> MockAnonymousTracking:
    return MockAnonymousTracking()
