from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List

from elementary.monitor.alerts.alert import Alert, SlackAlertMessageBuilder
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.fetchers.alerts.normalized_alert import CHANNEL_KEY
from elementary.utils.json_utils import try_load_json
from elementary.utils.models import alert_to_concise_name, get_shortened_model_name

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class GroupingType(Enum):
    BY_ALERT = "by_alert"
    BY_TABLE = "by_table"
    ALL = "all"


@dataclass(
    frozen=True, eq=True
)  # frozen+eq defined so we can use it as a dict key. Also, it's all Strings
class NotificationComponent:
    name_in_summary: str
    empty_section_content: str


@dataclass(frozen=True, eq=True)
class AlertGroupComponent(NotificationComponent):
    emoji_in_summary: str
    name_in_full: str
    emoji_in_full: str


ErrorComponent = AlertGroupComponent(
    name_in_summary="Errors",
    emoji_in_summary="exclamation",
    name_in_full="Error",
    emoji_in_full="exclamation",
    empty_section_content="No Errors",
)

WarningComponent = AlertGroupComponent(
    name_in_summary="Warning",
    emoji_in_summary="warning",
    name_in_full="Warning",
    emoji_in_full="warning",
    empty_section_content="No Warnings",
)

FailureComponent = AlertGroupComponent(
    name_in_summary="Failed",
    emoji_in_summary="small_red_triangle",
    name_in_full="Failed tests",
    emoji_in_full="X",
    empty_section_content="No Failures",
)


TagsComponent = NotificationComponent(
    name_in_summary="Tags", empty_section_content="No Tags"
)
OwnersComponent = NotificationComponent(
    name_in_summary="Owners", empty_section_content="No Owners"
)
SubsComponent = NotificationComponent(
    name_in_summary="Subscribers", empty_section_content="No Subscribers"
)


class GroupOfAlerts(SlackAlertMessageBuilder):
    def __init__(self, alerts: List[Alert], default_channel_destination: str):

        self.alerts = alerts

        self._sort_channel_destination(default_channel=default_channel_destination)
        self._fill_components_to_alerts(alerts)

        tags = self._fill_and_dedup_tags(alerts)
        self._components_to_attn_required: Dict[NotificationComponent, str] = {
            TagsComponent: tags
        }
        # self_components_to_attn_required is a magic dict that maps:
        #   OwnersComponent -> ", ".join(self.owners) ,
        #   SubsComponent -> self.subscribers .
        #   magic is enforced in self.__setattr__ .
        self._fill_and_dedup_owners_and_subs(
            alerts
        )  # we have to hold owners and subscribers explicitly to let DataMonitoring call the slackAPI with them.
        super().__init__()

    def __setattr__(self, key, value):
        if key == "owners":
            self._components_to_attn_required[OwnersComponent] = ", ".join(value)
        if key == "subscribers":
            self._components_to_attn_required[SubsComponent] = ", ".join(value)
        return super().__setattr__(key, value)

    def _fill_and_dedup_owners_and_subs(self, alerts):
        owners = set([])
        subscribers = set([])
        for al in alerts:
            if al.owners is not None:
                if isinstance(al.owners, list):
                    owners.update(al.owners)
                else:  # it's a string. could be comma delimited.
                    owners.update(al.owners.split(","))
            if al.subscribers is not None:
                if isinstance(al.subscribers, list):
                    subscribers.update(al.subscribers)
                else:  # it's a string. could be comma delimited.
                    subscribers.update(al.subscribers.split(","))
        self.owners = list(owners)
        self.subscribers = list(subscribers)

    def _fill_and_dedup_tags(self, alerts):
        tags = set([])
        for al in alerts:
            if al.tags is not None:
                if isinstance(al.tags, str):
                    tags_unjsoned = try_load_json(
                        al.tags
                    )  # tags is a string, comma delimited values
                    if (
                        tags_unjsoned is None
                    ):  # maybe a string, maybe some comma delimited strings
                        tags.update([x.strip() for x in al.tags.split(",")])
                    elif isinstance(tags_unjsoned, str):  # tags was a quoted string.
                        tags.update([x.strip() for x in al.tags.split(",")])
                    elif isinstance(tags_unjsoned, list):  # tags was a list of strings
                        tags.update(tags_unjsoned)
                elif isinstance(al.tags, list):
                    tags.update(al.tags)
        TAG_PREFIX = "#"
        formatted_tags = [
            tag if tag.startswith(TAG_PREFIX) else f"{TAG_PREFIX}{tag}" for tag in tags
        ]
        return ", ".join(formatted_tags)

    def _sort_channel_destination(self, default_channel):
        raise NotImplementedError

    def _fill_components_to_alerts(self, alerts):
        errors = []
        warnings = []
        failures = []
        for alert in self.alerts:
            if isinstance(alert, ModelAlert) or alert.status == "error":
                errors.append(alert)
            elif alert.status == "warn":
                warnings.append(alert)
            else:
                failures.append(alert)
        self._components_to_alerts: Dict[AlertGroupComponent, List[Alert]] = {
            FailureComponent: failures,
            WarningComponent: warnings,
            ErrorComponent: errors,
        }

    def to_slack(self):
        title_blocks = []  # title, [banner], number of passed or failed,
        title_blocks.append(self._title_block())
        banner_block = self._get_banner_block()
        if banner_block:
            title_blocks.append(banner_block)
        title_blocks.append(self._number_of_failed_block())
        self._add_title_to_slack_alert(title_blocks=title_blocks)

        # attention required : tags, owners, subscribers
        self._add_preview_to_slack_alert(
            preview_blocks=self._attention_required_blocks()
        )

        details_blocks = []
        for component, alerts_list in self._components_to_alerts.items():
            details_blocks.append(
                self.create_text_section_block(
                    f":{component.emoji_in_summary}: *{component.name_in_summary}*"
                )
            )
            details_blocks.append(self.create_divider_block())
            if len(alerts_list) == 0:
                text = f"_{component.empty_section_content}_"
            else:
                text = self._tabulate_list_of_alerts(alerts_list)
            details_blocks.append(self.create_text_section_block(text))
        self._add_blocks_as_attachments(details_blocks)

        return self.get_slack_message()

    def _title_block(self):
        return self.create_header_block(
            f":small_red_triangle_down: {self._title} ({len(self.alerts)} alerts)"
        )

    def _number_of_failed_block(self):
        # small_red_triangle: Falied: 36    |    :Warning: Warning: 3    |    :exclamation: Errors: 1
        fields = []
        all_components = list(self._components_to_alerts.items())
        all_components_but_last = all_components[:-1]
        for component, al_list in all_components_but_last:
            fields.append(
                f":{component.emoji_in_summary}: {component.name_in_summary}: {len(al_list)}    |"
            )
        component, al_list = all_components[-1]
        fields.append(
            (
                f":{component.emoji_in_summary}: {component.name_in_summary}: {len(al_list)}"
            )
        )

        return self.create_context_block(fields)

    def _get_banner_block(self):
        return None

    def _attention_required_blocks(self):
        preview_blocks = [
            self.create_text_section_block(":mega: *Attention required* :mega:")
        ]

        for component, val in self._components_to_attn_required.items():
            text = f"_{component.empty_section_content}_" if not val else val
            preview_blocks.append(
                self.create_text_section_block(f"*{component.name_in_summary}*: {text}")
            )

        preview_blocks.append(self.create_empty_section_block())

        return preview_blocks

    def _tabulate_list_of_alerts(self, al_list):
        ret = []
        for al in al_list:
            ret.append(self._get_tabulated_row_from_alert(al))
        return "\n".join(ret)

    def _get_tabulated_row_from_alert(self, alert: Alert):
        raise NotImplementedError

    def _had_channel_clashes(self):
        return False


class GroupOfAlertsByTable(GroupOfAlerts):
    def __init__(self, alerts: List[Alert], default_channel_destination: str):

        # sort out model unique id
        models = set([al.model_unique_id for al in alerts])
        if len(models) != 1:
            raise ValueError(
                f"failed initializing a GroupOfAlertsByTable, for alerts with multiple models: {list(models)}"
            )
        self._model = list(models)[0]
        self._title = get_shortened_model_name(self._model)
        super().__init__(alerts, default_channel_destination)

    def _sort_channel_destination(self, default_channel):
        """
        where do we send a group of alerts to?
        Definitions:
        1. "default_channel" is the project yaml level definition, over-rided by CLI if given
        2. "per alert" is the definition for tests (if exists), or for the related model (if exists).
        Sorting out:
        if grouping is "by table",
         if model has specific channels configured:
          - Send to the Model's configured channel.
         else
          - send it to the default channel
        """

        # Check for a model level configuration.
        model_specific_channel_config = None
        for alert in self.alerts:
            if isinstance(alert, ModelAlert):
                if alert.slack_channel:
                    model_specific_channel_config = alert.slack_channel
                    break
            model_meta_data = try_load_json(alert.model_meta)
            if model_meta_data and isinstance(model_meta_data, dict):
                model_specific_channel_config = model_meta_data.get(CHANNEL_KEY)
                break

        if model_specific_channel_config:
            self.channel_destination = model_specific_channel_config
        else:
            self.channel_destination = default_channel

    def _get_tabulated_row_from_alert(self, alert: Alert):
        detected_at = alert.detected_at
        if isinstance(detected_at, str):
            idx = detected_at.rfind("+")
            if idx > 0:
                detected_at = detected_at[:idx]
        if isinstance(detected_at, datetime):
            detected_at = detected_at.strftime(DATETIME_FORMAT)

        return f"{alert_to_concise_name(alert)} | {detected_at}"


class GroupOfAlertsByAll(GroupOfAlerts):
    def __init__(self, alerts: List[Alert], default_channel_destination: str):
        self._title = "Alerts Summary"
        super().__init__(alerts, default_channel_destination)

    def _sort_channel_destination(self, default_channel):
        """
        where do we send a group of alerts to?
        Definitions:
        1. "default_channel" is the project yaml level definition, over-rided by CLI if given
        :return:
        """
        self.channel_destination = default_channel

    def _get_tabulated_row_from_alert(self, alert: Alert):
        return f"{alert.model_unique_id} | {alert_to_concise_name(alert)}"


class GroupOfAlertsBySingleAlert(GroupOfAlerts):
    def _sort_channel_destination(self, default_channel):
        """
        where do we send a group of alerts to?
        Definitions:
        1. "default_channel" is the project yaml level definition, over-rided by CLI if given
        2. "per alert" is the definition for tests (if exists), or for the related model (if exists).
        Sorting out:

        if grouping is "by alert", test definition or model definition or CLI if given or project-yaml definition
        """
        if self.alerts[0].slack_channel:
            self.channel_destination = self.alerts[0].slack_channel
        else:
            self.channel_destination = default_channel

    def to_slack(self):
        return self.alerts[0].to_slack()


#
# class SlackMessageBuilder:
#     pass
#
# class SlackMessageThatInvolveMultipleRunResultsBuilder(SlackMessageBuilder)
#
# class ReportSummarySlackMessageBuilder(SlackMessageThatInvolveMultipleRunResultsBuilder):
#     pass
#
# class GeneralGroupOfAlerts(SlackMessageThatInvolveMultipleRunResultsBuilder):
#     pass
# class ByTableGroupOfAlert(GeneralGroupOfAlerts)
#     pass
#
# class ByAllGroupOfAlert(GeneralGroupOfAlerts)
#     pass


"""
Stuff to test:
- business logic of getting the config
    - mock some test_meta and model_meta 
- business logic of _group_alerts_per_config 
- manually play a bit with overriding configs in the project level

"""
