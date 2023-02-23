import json
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Union

from parametrization import Parametrization

from elementary.monitor.alerts.group_of_alerts import (
    ErrorComponent,
    FailureComponent,
    GroupOfAlerts,
    GroupOfAlertsByAll,
    GroupOfAlertsBySingleAlert,
    GroupOfAlertsByTable,
    OwnersComponent,
    SubsComponent,
    TagsComponent,
    WarningComponent,
)
from elementary.monitor.data_monitoring.data_monitoring_alerts import (
    DataMonitoringAlerts,
)
from elementary.monitor.fetchers.alerts.normalized_alert import CHANNEL_KEY

# what I use
# self.config.slack_group_alerts_by
# self.config.slack_channel_name
# self.execution_properties["had_group_by_all"] = (len(alls_group) > 0)
# self.execution_properties["had_group_by_table"] = (len(by_table_group) > 0)
# self.execution_properties["had_group_by_alert"] = (len(by_alert_group) > 0)
# al.slack_group_alerts_by
# al.model_unique_id


#####################
### Mock Classes ####
#####################
@dataclass
class MockAlert:
    status: Optional[str]  # should be in "warn", "error" , "fail[ure]"
    slack_group_alerts_by: Optional[str]
    model_unique_id: Optional[str]
    slack_channel: Optional[str]
    detected_at: Optional[Union[str, datetime]]
    model_meta: Optional[
        str
    ]  # this string should be a json of a dict that has or has not the key "channel"
    owners: Optional[List[str]]
    subscribers: Optional[List[str]]
    tags: Optional[str]


@dataclass
class MockConfig:
    slack_group_alerts_by: Optional[str]
    slack_channel_name: Optional[str]


@dataclass
class MockDataMonitoringAlerts:
    config: MockConfig
    execution_properties: Dict


def mock_data_monitoring_alerts(mock_config):
    return MockDataMonitoringAlerts(config=mock_config, execution_properties=dict())


###################
### Mock Data  ####
###################
DEFAULT_CHANNEL = "roi-playground"
OTHER_CHANNEL = "roi-playground-2"
MODEL_1 = "models.bla.model1"
MODEL_2 = "models.bla.model2"
MODEL_3 = "models.bla.model3"
OWNER_1 = "owner1@elementary-data.com"
OWNER_2 = "owner2@elementary-data.com"
OWNER_3 = "owner3@elementary-data.com"
TAGS_1 = "marketing, finance"
TAGS_2 = "finance, data-ops"
TAGS_3 = "marketing, finance, data-ops"
DETECTED_AT = "1992-11-11 20:00:03+0200"
EMPTY_DICT = json.dumps(dict())
OTHER_CHANNEL_IN_DICT = json.dumps({CHANNEL_KEY: OTHER_CHANNEL})

AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING = MockAlert(
    status="warn",
    slack_group_alerts_by=None,
    model_unique_id=MODEL_1,
    slack_channel=None,
    detected_at=DETECTED_AT,
    model_meta=EMPTY_DICT,
    owners=[OWNER_1, OWNER_2],
    subscribers=[],
    tags=TAGS_1,
)

AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING = MockAlert(
    status="fail",
    slack_group_alerts_by=None,
    model_unique_id=MODEL_1,
    slack_channel=OTHER_CHANNEL,
    detected_at=DETECTED_AT,
    model_meta="{}",
    owners=[OWNER_1, OWNER_3],
    subscribers=[],
    tags=TAGS_2,
)

AL_FAIL_MODEL2_NO_CHANNEL_NO_GROUPING = MockAlert(
    status="fail",
    slack_group_alerts_by=None,
    model_unique_id=MODEL_2,
    slack_channel=None,
    detected_at=DETECTED_AT,
    model_meta=EMPTY_DICT,
    owners=[OWNER_1],
    subscribers=[],
    tags=TAGS_3,
)
AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_ALERT = MockAlert(
    status="fail",
    slack_group_alerts_by="by_alert",
    model_unique_id=MODEL_2,
    slack_channel=None,
    detected_at=DETECTED_AT,
    model_meta=EMPTY_DICT,
    owners=[OWNER_1],
    subscribers=[],
    tags=TAGS_1,
)
AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE = MockAlert(
    status="fail",
    slack_group_alerts_by="by_table",
    model_unique_id=MODEL_2,
    slack_channel=None,
    detected_at=DETECTED_AT,
    model_meta=OTHER_CHANNEL_IN_DICT,
    owners=[OWNER_1],
    subscribers=[],
    tags=TAGS_1,
)
AL_ERROR_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE = MockAlert(
    status="error",
    slack_group_alerts_by="by_table",
    model_unique_id=MODEL_2,
    slack_channel=None,
    detected_at=DETECTED_AT,
    model_meta=EMPTY_DICT,
    owners=[OWNER_1],
    subscribers=[],
    tags=TAGS_1,
)
AL_ERROR_MODEL3_NO_CHANNEL_WITH_GROUPING_BY_ALERT = MockAlert(
    status="error",
    slack_group_alerts_by="by_alert",
    model_unique_id=MODEL_3,
    slack_channel=None,
    detected_at=DETECTED_AT,
    model_meta=EMPTY_DICT,
    owners=[OWNER_1],
    subscribers=[],
    tags=TAGS_1,
)


#############
### Utils ###
#############
def check_eq_group_alerts(grp1: GroupOfAlerts, grp2: GroupOfAlerts):
    ret = grp1.alerts == grp2.alerts  # same alerts
    ret &= grp1._components_to_alerts == grp2._components_to_alerts  # same mappings
    ret &= (
        grp1._components_to_attn_required == grp2._components_to_attn_required
    )  # same owners, tags, subscribers

    return ret


#################
## Test Cases ###
#################


@Parametrization.autodetect_parameters()
@Parametrization.default_parameters(
    default_channel=DEFAULT_CHANNEL,
    default_grouping="by_alert",
    expected_execution_properties=None,
)
@Parametrization.case(
    name="empty_list_goes_to_empty_list",
    list_of_alerts=[],
    expected_alert_groups=[],
    expected_execution_properties={
        "had_group_by_alert": False,
        "had_group_by_table": False,
        "had_group_by_all": False,
    },
)
@Parametrization.case(
    name="one_warning_goes_to_one_warning",
    list_of_alerts=[AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING],
    expected_alert_groups=[
        GroupOfAlertsBySingleAlert(
            alerts=[AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING],
            default_channel_destination=DEFAULT_CHANNEL,
        )
    ],
    expected_execution_properties={
        "had_group_by_alert": True,
        "had_group_by_table": False,
        "had_group_by_all": False,
    },
)
@Parametrization.case(
    name="one_fail_group_by_all_channel_selection_is_default",
    default_grouping="all",
    list_of_alerts=[AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING],
    expected_alert_groups=[
        GroupOfAlertsByAll(
            alerts=[AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING],
            default_channel_destination=DEFAULT_CHANNEL,
        )
    ],
    expected_execution_properties={
        "had_group_by_alert": False,
        "had_group_by_table": False,
        "had_group_by_all": True,
    },
)
@Parametrization.case(
    name="one_fail_one_warn_same_model_group_by_table_groups_them_together",
    default_grouping="by_table",
    list_of_alerts=[
        AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING,
        AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING,
    ],
    expected_alert_groups=[
        GroupOfAlertsByTable(
            alerts=[
                AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING,
                AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING,
            ],
            default_channel_destination=DEFAULT_CHANNEL,
        )
    ],
    expected_execution_properties={
        "had_group_by_alert": False,
        "had_group_by_table": True,
        "had_group_by_all": False,
    },
)
@Parametrization.case(
    name="one_fail_one_warn_same_table_one_other_table_group_by_table_groups_them_to_2_groups",
    default_grouping="by_table",
    list_of_alerts=[
        AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING,
        AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING,
        AL_FAIL_MODEL2_NO_CHANNEL_NO_GROUPING,
    ],
    expected_alert_groups=[
        GroupOfAlertsByTable(
            alerts=[
                AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING,
                AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING,
            ],
            default_channel_destination=DEFAULT_CHANNEL,
        ),
        GroupOfAlertsByTable(
            alerts=[AL_FAIL_MODEL2_NO_CHANNEL_NO_GROUPING],
            default_channel_destination=DEFAULT_CHANNEL,
        ),
    ],
    expected_execution_properties={
        "had_group_by_alert": False,
        "had_group_by_table": True,
        "had_group_by_all": False,
    },
)
@Parametrization.case(
    name="two_alerts_on_model_1_two_alerts_on_model_2_default_grouping_is_by_table_by_one_alert_has_group_by_alert",
    default_grouping="by_table",
    list_of_alerts=[
        AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING,
        AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING,
        AL_FAIL_MODEL2_NO_CHANNEL_NO_GROUPING,
        AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_ALERT,
    ],
    expected_alert_groups=[
        GroupOfAlertsByTable(
            alerts=[
                AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING,
                AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING,
            ],
            default_channel_destination=DEFAULT_CHANNEL,
        ),
        GroupOfAlertsByTable(
            alerts=[AL_FAIL_MODEL2_NO_CHANNEL_NO_GROUPING],
            default_channel_destination=DEFAULT_CHANNEL,
        ),
        GroupOfAlertsBySingleAlert(
            alerts=[AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_ALERT],
            default_channel_destination=DEFAULT_CHANNEL,
        ),
    ],
    expected_execution_properties={
        "had_group_by_alert": True,
        "had_group_by_table": True,
        "had_group_by_all": False,
    },
)
@Parametrization.case(
    name="default_grouping_all_and_overrides_existing_by_alert_and_by_table_to_2_out_of_3_of_model_2_s_alerts",
    default_grouping="all",
    list_of_alerts=[
        AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING,
        AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING,
        AL_FAIL_MODEL2_NO_CHANNEL_NO_GROUPING,
        AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_ALERT,
        AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE,
        AL_ERROR_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE,
        AL_ERROR_MODEL3_NO_CHANNEL_WITH_GROUPING_BY_ALERT,
    ],
    expected_alert_groups=[
        GroupOfAlertsByAll(
            alerts=[
                AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING,
                AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING,
                AL_FAIL_MODEL2_NO_CHANNEL_NO_GROUPING,
            ],
            default_channel_destination=DEFAULT_CHANNEL,
        ),
        GroupOfAlertsByTable(
            alerts=[
                AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE,
                AL_ERROR_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE,
            ],
            default_channel_destination=DEFAULT_CHANNEL,
        ),
        GroupOfAlertsBySingleAlert(
            alerts=[AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_ALERT],
            default_channel_destination=DEFAULT_CHANNEL,
        ),
        GroupOfAlertsBySingleAlert(
            alerts=[AL_ERROR_MODEL3_NO_CHANNEL_WITH_GROUPING_BY_ALERT],
            default_channel_destination=DEFAULT_CHANNEL,
        ),
    ],
    expected_execution_properties={
        "had_group_by_alert": True,
        "had_group_by_table": True,
        "had_group_by_all": True,
    },
)
def test_grouping_logic(
    default_channel,
    default_grouping,
    list_of_alerts,
    expected_alert_groups,
    expected_execution_properties,
):
    # init
    conf = MockConfig(
        slack_group_alerts_by=default_grouping, slack_channel_name=default_channel
    )
    data_monitoring_alerts = mock_data_monitoring_alerts(conf)

    # business logic
    list_of_groups = DataMonitoringAlerts._group_alerts_per_config(
        data_monitoring_alerts, list_of_alerts
    )

    # assertions
    if expected_alert_groups is not None:
        assert len(list_of_groups) == len(expected_alert_groups)
        for grp1, grp2 in zip(list_of_groups, expected_alert_groups):
            assert check_eq_group_alerts(grp1, grp2)

    if expected_execution_properties is not None:
        assert (
            data_monitoring_alerts.execution_properties == expected_execution_properties
        )


@Parametrization.autodetect_parameters()
@Parametrization.default_parameters(
    grouping_class=GroupOfAlertsByAll,
    default_channel=DEFAULT_CHANNEL,
    expected_owners=None,
    expected_tags=None,
    expected_subs=None,
    expected_warnings=None,
    expected_fails=None,
    expected_errors=None,
    expected_channel=None,
)
@Parametrization.case(
    name="single_alert_no_channel_goes_to_default_channel",
    grouping_class=GroupOfAlertsBySingleAlert,
    alerts_list=[AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING],
    expected_channel=DEFAULT_CHANNEL,
)
@Parametrization.case(
    name="single_alert_with_non_default_channel_goes_to_non_default_channel",
    grouping_class=GroupOfAlertsBySingleAlert,
    alerts_list=[AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING],
    expected_channel=OTHER_CHANNEL,
)
@Parametrization.case(
    name="group_by_table_forces_use_of_the_model_channel",
    grouping_class=GroupOfAlertsByTable,
    alerts_list=[
        AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE,
        AL_ERROR_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE,
    ],
    expected_channel=OTHER_CHANNEL,
)
@Parametrization.case(
    name="group_by_all_forces_use_of_the_default_channel",
    grouping_class=GroupOfAlertsByAll,
    alerts_list=[
        AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE,
        AL_ERROR_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE,
    ],
    expected_channel=DEFAULT_CHANNEL,
)
@Parametrization.case(
    name="owners_are_deduplicated",
    grouping_class=GroupOfAlertsByAll,
    alerts_list=[
        AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING,
        AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING,
    ],
    expected_owners=[OWNER_1, OWNER_2, OWNER_3],
)
@Parametrization.case(
    name="tags_are_deduplicated",
    grouping_class=GroupOfAlertsByAll,
    alerts_list=[
        AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING,
        AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING,
    ],
    expected_tags=TAGS_3,
)
@Parametrization.case(
    name="errors_warnings_and_fails_are_routed_properly",
    grouping_class=GroupOfAlertsByAll,
    alerts_list=[
        AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING,
        AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING,
        AL_FAIL_MODEL2_NO_CHANNEL_NO_GROUPING,
        AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_ALERT,
        AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE,
        AL_ERROR_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE,
        AL_ERROR_MODEL3_NO_CHANNEL_WITH_GROUPING_BY_ALERT,
    ],
    expected_errors=[
        AL_ERROR_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE,
        AL_ERROR_MODEL3_NO_CHANNEL_WITH_GROUPING_BY_ALERT,
    ],
    expected_warnings=[AL_WARN_MODEL1_NO_CHANNEL_NO_GROUPING],
    expected_fails=[
        AL_FAIL_MODEL1_WITH_CHANNEL_NO_GROUPING,
        AL_FAIL_MODEL2_NO_CHANNEL_NO_GROUPING,
        AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_ALERT,
        AL_FAIL_MODEL2_NO_CHANNEL_WITH_GROUPING_BY_TABLE,
    ],
)
def test_alert_group_construction(
    grouping_class,
    alerts_list,
    default_channel,
    expected_owners,
    expected_tags,
    expected_subs,
    expected_warnings,
    expected_fails,
    expected_errors,
    expected_channel,
):
    # business logic
    alerts_group = grouping_class(alerts_list, default_channel)

    # assertions
    if expected_owners is not None:
        assert sorted(
            alerts_group._components_to_attn_required[OwnersComponent].split(", ")
        ) == sorted(expected_owners)
    if expected_tags is not None:
        assert sorted(
            [
                x.replace("#", "")
                for x in alerts_group._components_to_attn_required[TagsComponent].split(
                    ", "
                )
            ]
        ) == sorted(expected_tags.split(", "))
    if expected_subs is not None:
        assert sorted(
            alerts_group._components_to_attn_required[SubsComponent]
        ) == sorted(expected_subs)
    if expected_errors is not None:
        assert alerts_group._components_to_alerts[ErrorComponent] == expected_errors
    if expected_warnings is not None:
        assert alerts_group._components_to_alerts[WarningComponent] == expected_warnings
    if expected_fails is not None:
        assert alerts_group._components_to_alerts[FailureComponent] == expected_fails
    if expected_channel is not None:
        assert alerts_group.channel_destination == expected_channel
