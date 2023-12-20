from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
from parametrization import Parametrization

from elementary.monitor.data_monitoring.schema import (
    InvalidSelectorError,
    ResourceType,
    SelectorFilterSchema,
    Status,
)
from elementary.monitor.data_monitoring.selector_filter import SelectorFilter
from tests.mocks.anonymous_tracking_mock import MockAnonymousTracking
from tests.mocks.config_mock import MockConfig


def test_parse_selector_with_user_dbt_runner_no_models(
    dbt_runner_no_models_mock, anonymous_tracking_mock
):
    config = MockConfig("mock_project_dir")

    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        tracking=anonymous_tracking_mock,
        config=config,
        selector="mock:selector",
    )

    assert data_monitoring_filter_with_user_dbt_runner.get_filter().node_names == []
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().selector
        == "mock:selector"
    )


def test_parse_selector_with_user_dbt_runner_with_models(
    dbt_runner_with_models_mock, anonymous_tracking_mock
):
    config = MockConfig("mock_project_dir")

    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        tracking=anonymous_tracking_mock,
        config=config,
        selector="mock:selector",
    )

    assert data_monitoring_filter_with_user_dbt_runner.get_filter().node_names == [
        "node_name_1",
        "node_name_2",
    ]
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().selector
        == "mock:selector"
    )


def test_parse_selector_without_user_dbt_runner(anonymous_tracking_mock):
    config = MockConfig()

    # tag selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        config=config,
        tracking=anonymous_tracking_mock,
        selector="tag:mock_tag",
    )
    assert data_monitoring_filter_with_user_dbt_runner.get_filter().tag == "mock_tag"
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().selector
        == "tag:mock_tag"
    )

    # owner selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        config=config,
        tracking=anonymous_tracking_mock,
        selector="config.meta.owner:mock_owner",
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().owner == "mock_owner"
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().selector
        == "config.meta.owner:mock_owner"
    )

    # model selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        config=config,
        tracking=anonymous_tracking_mock,
        selector="model:mock_model",
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().model == "mock_model"
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().selector
        == "model:mock_model"
    )

    # status selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        config=config,
        tracking=anonymous_tracking_mock,
        selector="statuses:fail,error",
    )
    assert data_monitoring_filter_with_user_dbt_runner.get_filter().statuses == [
        Status.FAIL,
        Status.ERROR,
    ]
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().selector
        == "statuses:fail,error"
    )

    # resource type selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        config=config,
        tracking=anonymous_tracking_mock,
        selector="resource_types:model",
    )
    assert data_monitoring_filter_with_user_dbt_runner.get_filter().resource_types == [
        ResourceType.MODEL
    ]
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().selector
        == "resource_types:model"
    )

    # invocation_id selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        config=config,
        tracking=anonymous_tracking_mock,
        selector="invocation_id:mock_invocation_id",
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().invocation_id
        == "mock_invocation_id"
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().selector
        == "invocation_id:mock_invocation_id"
    )

    # invocation_time selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        config=config,
        tracking=anonymous_tracking_mock,
        selector="invocation_time:2023-02-08 10:00:00",
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().invocation_time
        is not None
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().selector
        == "invocation_time:2023-02-08 10:00:00"
    )

    # invalid_invocation_time selector
    with pytest.raises(ValueError):
        data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
            config=config,
            tracking=anonymous_tracking_mock,
            selector="invocation_time:2023-32-32",
        )

    # last_invocation selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        config=config,
        tracking=anonymous_tracking_mock,
        selector="last_invocation",
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().last_invocation is True
    )
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().selector
        == "last_invocation"
    )

    # unsupported selector
    data_monitoring_filter_with_user_dbt_runner = SelectorFilter(
        config=config,
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
    assert (
        data_monitoring_filter_with_user_dbt_runner.get_filter().selector
        == "blabla:blublu"
    )


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="None", selector_filter=SelectorFilterSchema(), should_raise=False
)
@Parametrization.case(
    name="report filter1",
    selector_filter=SelectorFilterSchema(selector="invocation_id:mock_invocation_id"),
    should_raise=False,
)
@Parametrization.case(
    name="report filter2",
    selector_filter=SelectorFilterSchema(
        selector="invocation_time:mock_invocation_time"
    ),
    should_raise=False,
)
@Parametrization.case(
    name="report filter3",
    selector_filter=SelectorFilterSchema(selector="last_invocation"),
    should_raise=False,
)
@Parametrization.case(
    name="alerts filter1",
    selector_filter=SelectorFilterSchema(selector="model=blabla"),
    should_raise=True,
)
@Parametrization.case(
    name="alerts filter2",
    selector_filter=SelectorFilterSchema(selector="tag=blabla"),
    should_raise=True,
)
@Parametrization.case(
    name="alerts filter3",
    selector_filter=SelectorFilterSchema(selector="statuses=blabla"),
    should_raise=True,
)
def test_validate_report_selector(selector_filter, should_raise):
    if should_raise:
        with pytest.raises(InvalidSelectorError):
            selector_filter.validate_report_selector()
    else:
        selector_filter.validate_report_selector()


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="None", selector_filter=SelectorFilterSchema(), should_raise=False
)
@Parametrization.case(
    name="report filter1",
    selector_filter=SelectorFilterSchema(selector="invocation_id:mock_invocation_id"),
    should_raise=True,
)
@Parametrization.case(
    name="report filter2",
    selector_filter=SelectorFilterSchema(
        selector="invocation_time:mock_invocation_time"
    ),
    should_raise=True,
)
@Parametrization.case(
    name="report filter3",
    selector_filter=SelectorFilterSchema(selector="last_invocation"),
    should_raise=True,
)
@Parametrization.case(
    name="alerts filter1",
    selector_filter=SelectorFilterSchema(selector="model=blabla"),
    should_raise=False,
)
@Parametrization.case(
    name="alerts filter2",
    selector_filter=SelectorFilterSchema(selector="tag=blabla"),
    should_raise=False,
)
@Parametrization.case(
    name="alerts filter3",
    selector_filter=SelectorFilterSchema(selector="statuses=blabla"),
    should_raise=False,
)
def test_validate_alerts_selector(selector_filter, should_raise):
    if should_raise:
        with pytest.raises(InvalidSelectorError):
            selector_filter.validate_alert_selector()
    else:
        selector_filter.validate_alert_selector()


@pytest.fixture
def anonymous_tracking_mock() -> MockAnonymousTracking:
    return MockAnonymousTracking()


@pytest.fixture(scope="function")
def dbt_runner_no_models_mock() -> Generator[MagicMock, None, None]:
    with patch("elementary.clients.dbt.dbt_runner.DbtRunner.ls") as mock_ls:
        mock_ls.return_value = []
        yield mock_ls


@pytest.fixture(scope="function")
def dbt_runner_with_models_mock() -> Generator[MagicMock, None, None]:
    with patch("elementary.clients.dbt.dbt_runner.DbtRunner.ls") as mock_ls:
        mock_ls.return_value = ["node_name_1", "node_name_2"]
        yield mock_ls
