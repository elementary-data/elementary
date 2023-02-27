from datetime import datetime
from enum import Enum
from typing import Dict, List

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.clients.slack.slack_message_builder import SlackMessageBuilder
from elementary.monitor.alerts.alert import Alert, SlackAlertMessageBuilder
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.schema.alert_group_component import NotificationComponent, AlertGroupComponent
from elementary.monitor.fetchers.alerts.normalized_alert import CHANNEL_KEY
from elementary.utils.json_utils import try_load_json, list_of_lists_of_strings_to_comma_delimited_unique_strings
from elementary.utils.models import alert_to_concise_name, get_shortened_model_name

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class GroupingType(Enum):
    BY_ALERT = "alert"
    BY_TABLE = "table"


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
        self._fill_components_to_alerts()
        hashtag = SlackMessageBuilder._HASHTAG
        self._components_to_attention_required: Dict[AlertGroupComponent, str] = dict()
        self._components_to_attention_required[TagsComponent] = list_of_lists_of_strings_to_comma_delimited_unique_strings([alert.tags for alert in alerts], prefix=hashtag)
        self._components_to_attention_required[OwnersComponent] = list_of_lists_of_strings_to_comma_delimited_unique_strings([alert.owners for alert in alerts])
        self._components_to_attention_required[SubsComponent] = list_of_lists_of_strings_to_comma_delimited_unique_strings([alert.subscribers for alert in alerts])

        super().__init__()


    def set_owners(self, owners: list[str]):
        self._components_to_attention_required[OwnersComponent] = ", ".join(owners)

    def set_subscribers(self, subscribers: list[str]):
        self._components_to_attention_required[SubsComponent] = ", ".join(subscribers)

    def _sort_channel_destination(self, default_channel):
        raise NotImplementedError

    def _fill_components_to_alerts(self):
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

    def to_slack(self) -> SlackMessageSchema:
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
            f":small_red_triangle: {self._title} ({len(self.alerts)} alerts)"
        )

    def _number_of_failed_block(self):
        # small_red_triangle: Falied: 36    |    :Warning: Warning: 3    |    :exclamation: Errors: 1
        fields = []
        all_components = list(self._components_to_alerts.items())
        all_components_but_last = all_components[:-1]
        for component, alert_list in all_components_but_last:
            fields.append(
                f":{component.emoji_in_summary}: {component.name_in_summary}: {len(alert_list)}    |"
            )
        component, alert_list = all_components[-1]
        fields.append(
            (
                f":{component.emoji_in_summary}: {component.name_in_summary}: {len(alert_list)}"
            )
        )

        return self.create_context_block(fields)

    def _get_banner_block(self):
        return None

    def _attention_required_blocks(self):
        preview_blocks = [
            self.create_text_section_block(":mega: *Attention required* :mega:")
        ]

        for component, val in self._components_to_attention_required.items():
            text = f"_{component.empty_section_content}_" if not val else val
            preview_blocks.append(
                self.create_text_section_block(f"*{component.name_in_summary}*: {text}")
            )

        preview_blocks.append(self.create_empty_section_block())

        return preview_blocks

    def _tabulate_list_of_alerts(self, alert_list):
        ret = []
        for alert in alert_list:
            ret.append(self._get_tabulated_row_from_alert(alert))
        return "\n".join(ret)

    def _get_tabulated_row_from_alert(self, alert: Alert):
        raise NotImplementedError

    def _had_channel_clashes(self):
        return False


class GroupOfAlertsByTable(GroupOfAlerts):
    def __init__(self, alerts: List[Alert], default_channel_destination: str):

        # sort out model unique id
        models = set([alert.model_unique_id for alert in alerts])
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

    def set_owners(self, owners):
        self.alerts[0].owners = owners

    def set_subscribers(self, subscribers):
        self.alerts[0].subscribers=subscribers

    def set_tags(self, tags):
        self.alerts[0].tags=tags
