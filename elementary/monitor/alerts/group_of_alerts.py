from enum import Enum
from typing import Dict, List, Optional

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.clients.slack.slack_message_builder import SlackMessageBuilder
from elementary.monitor.alerts.alert import Alert, SlackAlertMessageBuilder
from elementary.monitor.alerts.model import ModelAlert
from elementary.monitor.alerts.report_link_utils import get_model_test_runs_link
from elementary.monitor.alerts.schema.alert import ReportLinkData
from elementary.monitor.alerts.schema.alert_group_component import (
    AlertGroupComponent,
    NotificationComponent,
)
from elementary.monitor.fetchers.alerts.normalized_alert import CHANNEL_KEY
from elementary.utils.json_utils import (
    list_of_lists_of_strings_to_comma_delimited_unique_strings,
)
from elementary.utils.models import get_shortened_model_name

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class GroupingType(Enum):
    BY_ALERT = "alert"
    BY_TABLE = "table"


ModelErrorComponent = AlertGroupComponent(
    name_in_summary="Model errors",
    emoji_in_summary="X",
    name_in_full="Model errors",
    emoji_in_full="X",
)

TestErrorComponent = AlertGroupComponent(
    name_in_summary="Test errors",
    emoji_in_summary="exclamation",
    name_in_full="Test errors",
    emoji_in_full="exclamation",
)

TestWarningComponent = AlertGroupComponent(
    name_in_summary="Test warnings",
    emoji_in_summary="warning",
    name_in_full="Test warnings",
    emoji_in_full="warning",
)

TestFailureComponent = AlertGroupComponent(
    name_in_summary="Test failures",
    emoji_in_summary="small_red_triangle",
    name_in_full="Test failures",
    emoji_in_full="small_red_triangle",
)

TagsComponent = NotificationComponent(
    order=0, name_in_summary="Tags", empty_section_content="No tags"
)
OwnersComponent = NotificationComponent(
    order=1, name_in_summary="Owners", empty_section_content="No owners"
)
SubsComponent = NotificationComponent(
    order=2, name_in_summary="Subscribers", empty_section_content="No subscribers"
)


class GroupOfAlerts:
    def __init__(
        self,
        alerts: List[Alert],
        default_channel_destination: str,
        override_slack_channel: bool,
        env: str = "dev",
    ):
        self.alerts = alerts
        self._title = self._get_title()
        self._sort_channel_destination(
            default_channel=default_channel_destination,
            override_channel=override_slack_channel,
        )
        self._fill_components_to_alerts()
        hashtag = SlackMessageBuilder._HASHTAG
        self._components_to_attention_required: Dict[
            NotificationComponent, str
        ] = dict()
        self._components_to_attention_required[
            TagsComponent
        ] = list_of_lists_of_strings_to_comma_delimited_unique_strings(
            [alert.tags or [] for alert in alerts], prefix=hashtag
        )
        self._components_to_attention_required[
            OwnersComponent
        ] = list_of_lists_of_strings_to_comma_delimited_unique_strings(
            [alert.owners or [] for alert in alerts]
        )
        self._components_to_attention_required[
            SubsComponent
        ] = list_of_lists_of_strings_to_comma_delimited_unique_strings(
            [alert.subscribers or [] for alert in alerts]
        )

        self._message_builder = (
            SlackAlertMessageBuilder()
        )  # only place it should be used is inside to_slack
        self._env = env

    def set_owners(self, owners: List[str]):
        self._components_to_attention_required[OwnersComponent] = ", ".join(owners)

    def set_subscribers(self, subscribers: List[str]):
        self._components_to_attention_required[SubsComponent] = ", ".join(subscribers)

    def _sort_channel_destination(self, default_channel, override_channel):
        raise NotImplementedError

    def _fill_components_to_alerts(self):
        test_errors = []
        test_warnings = []
        test_failures = []
        model_errors: List[Alert] = []
        for alert in self.alerts:
            if isinstance(alert, ModelAlert):
                model_errors.append(alert)
            elif alert.status == "error":
                test_errors.append(alert)
            elif alert.status == "warn":
                test_warnings.append(alert)
            else:
                test_failures.append(alert)
        self._components_to_alerts: Dict[AlertGroupComponent, List[Alert]] = dict()
        if model_errors:
            self._components_to_alerts[ModelErrorComponent] = model_errors
        if test_failures:
            self._components_to_alerts[TestFailureComponent] = test_failures
        if test_warnings:
            self._components_to_alerts[TestWarningComponent] = test_warnings
        if test_errors:
            self._components_to_alerts[TestErrorComponent] = test_errors

    def to_slack(self) -> SlackMessageSchema:
        title_blocks = []  # title, [banner], number of passed or failed,
        title_blocks.append(
            self._message_builder.create_header_block(self._title_block())
        )

        # summary of number of failed, errors, etc.
        fields_summary: List[str] = []
        # this would have been a loop but the order matters.
        alert_list = self._components_to_alerts.get(ModelErrorComponent)
        if alert_list:
            fields_summary.append(
                f":{ModelErrorComponent.emoji_in_summary}: {ModelErrorComponent.name_in_summary}: {len(alert_list)}    |"
            )
        alert_list = self._components_to_alerts.get(TestFailureComponent)
        if alert_list:
            fields_summary.append(
                f":{TestFailureComponent.emoji_in_summary}: {TestFailureComponent.name_in_summary}: {len(alert_list)}    |"
            )
        alert_list = self._components_to_alerts.get(TestWarningComponent)
        if alert_list:
            fields_summary.append(
                f":{TestWarningComponent.emoji_in_summary}: {TestWarningComponent.name_in_summary}: {len(alert_list)}    |"
            )
        alert_list = self._components_to_alerts.get(TestErrorComponent)
        if alert_list:
            fields_summary.append(
                f":{TestErrorComponent.emoji_in_summary}: {TestErrorComponent.name_in_summary}: {len(alert_list)}"
            )
        title_blocks.append(self._message_builder.create_context_block(fields_summary))

        report_slack_link = self._get_report_link()
        if report_slack_link:
            report_link = self._message_builder.create_context_block(
                [
                    f"<{report_slack_link.url}|{report_slack_link.text}>",
                ],
            )
            title_blocks.append(report_link)

        self._message_builder.add_title_to_slack_alert(title_blocks=title_blocks)

        # attention required : tags, owners, subscribers
        preview_blocks = [
            self._message_builder.create_text_section_block(block)
            for block in self._attention_required_blocks()
        ] + [self._message_builder.create_empty_section_block()]
        self._message_builder.add_preview_to_slack_alert(preview_blocks=preview_blocks)

        details_blocks = []
        for component, alerts_list in self._components_to_alerts.items():
            details_blocks.append(
                self._message_builder.create_text_section_block(
                    f"*{component.name_in_summary}*"
                )
            )
            details_blocks.append(self._message_builder.create_divider_block())
            if component == ModelErrorComponent:
                block_header = self._message_builder.create_context_block(
                    self._get_model_error_block_header()
                )
                block_body = self._message_builder.create_text_section_block(
                    self._get_model_error_block_body()
                )
                details_blocks.extend([block_header, block_body])
            else:
                rows = self._tabulate_list_of_alerts(alerts_list)
                text = "\n".join(
                    [f":{component.emoji_in_summary}: {row}" for row in rows]
                )
                details_blocks.append(
                    self._message_builder.create_text_section_block(text)
                )
        self._message_builder._add_blocks_as_attachments(details_blocks)

        return self._message_builder.get_slack_message()

    def _title_block(self) -> str:
        return f":small_red_triangle: {self._title}"

    def _get_model_error_block_header(self) -> List:
        model_error_alert_list = self._components_to_alerts[ModelErrorComponent]
        if len(model_error_alert_list) == 0:
            return []
        result = []
        for model_error_alert in model_error_alert_list:
            if not isinstance(model_error_alert, ModelAlert):
                raise Exception(
                    f"Unexpected type in model error alerts list: {type(model_error_alert)}"
                )

            if model_error_alert.message:
                result.extend(["*Result message*"])
        return result

    def _get_model_error_block_body(self) -> str:
        model_error_alert_list = self._components_to_alerts[ModelErrorComponent]
        if len(model_error_alert_list) == 0:
            return ""
        for model_error_alert in model_error_alert_list:
            if not isinstance(model_error_alert, ModelAlert):
                raise Exception(
                    f"Unexpected type in model error alerts list: {type(model_error_alert)}"
                )

            if model_error_alert.message:
                return f"```{model_error_alert.message.strip()}```"
        return ""

    def _attention_required_blocks(self):
        preview_blocks = []

        for component, val in sorted(
            self._components_to_attention_required.items(), key=lambda x: x[0].order
        ):
            text = f"_{component.empty_section_content}_" if not val else val
            preview_blocks.append(f"*{component.name_in_summary}*: {text}")

        return preview_blocks

    def _tabulate_list_of_alerts(self, alert_list) -> List[str]:
        rows = []
        for alert in alert_list:
            rows.append(self._get_tabulated_row_from_alert(alert))
        return rows

    def _get_tabulated_row_from_alert(self, alert: Alert):
        raise NotImplementedError

    def _get_title(self):
        return None

    def _get_report_link(self) -> Optional[ReportLinkData]:
        return None


class GroupOfAlertsByTable(GroupOfAlerts):
    def __init__(
        self,
        alerts: List[Alert],
        default_channel_destination: str,
        override_slack_channel: bool,
        env: str = "dev",
        report_url: Optional[str] = None,
    ):
        # sort out model unique id
        models = set([alert.model_unique_id for alert in alerts])
        if len(models) != 1:
            raise ValueError(
                f"failed initializing a GroupOfAlertsByTable, for alerts with multiple models: {list(models)}"
            )

        self._report_url = report_url
        self._model_unique_id = list(models)[0]
        self._model = get_shortened_model_name(self._model_unique_id)
        self._db = alerts[0].database_name
        self._schema = alerts[0].schema_name
        super().__init__(
            alerts, default_channel_destination, override_slack_channel, env
        )

    def _get_title(self) -> str:
        return f"Table issues detected - {self._model}"

    def _sort_channel_destination(self, default_channel, override_channel):
        """
        where do we send a group of alerts to?
        Definitions:
        1. "default_channel" is the project yaml level definition, overridden by CLI if given
        2. override_channel dictates whether the default channel should override the alert-specific channel
        3. "per alert" is the definition for tests (if exists), or for the related model (if exists).
        Sorting out:
        if grouping is "by table",
         if model has specific channels configured:
          - Send to the Model's configured channel.
         else
          - send it to the default channel
        """

        # if override_channel is set, we just use the default channel
        if override_channel:
            self.channel_destination = default_channel
            return

        # Check for a model level configuration.
        model_specific_channel_config = None
        for alert in self.alerts:
            if isinstance(alert, ModelAlert):
                if alert.slack_channel:
                    model_specific_channel_config = alert.slack_channel
                    break

            model_specific_channel_config = alert.model_meta.get(CHANNEL_KEY)
            if model_specific_channel_config:
                break

        if model_specific_channel_config:
            self.channel_destination = model_specific_channel_config
        else:
            self.channel_destination = default_channel

    def _get_tabulated_row_from_alert(self, alert: Alert) -> str:
        return alert.concise_name

    def _attention_required_blocks(self):
        preview_blocks = [f"*{self._db}.{self._schema}.{self._model}*"]
        preview_blocks.extend(
            super(GroupOfAlertsByTable, self)._attention_required_blocks()
        )
        return preview_blocks

    def _get_report_link(self) -> Optional[ReportLinkData]:
        # No report link when there is only model error
        if self._components_to_alerts.get(ModelErrorComponent) is None:
            return get_model_test_runs_link(self._report_url, self._model_unique_id)
        return None


class GroupOfAlertsBySingleAlert(GroupOfAlerts):
    def _sort_channel_destination(self, default_channel, override_channel):
        """
        where do we send a group of alerts to?
        Definitions:
        1. "default_channel" is the project yaml level definition, overridden by CLI if given
        2. "per alert" is the definition for tests (if exists), or for the related model (if exists).
        Sorting out:

        if grouping is "by alert", test definition or model definition or CLI if given or project-yaml definition
        """
        if self.alerts[0].slack_channel and not override_channel:
            self.channel_destination = self.alerts[0].slack_channel
        else:
            self.channel_destination = default_channel

    def to_slack(self):
        return self.alerts[0].to_slack()

    def set_owners(self, owners: List[str]):
        self.alerts[0].owners = owners

    def set_subscribers(self, subscribers: List[str]):
        self.alerts[0].subscribers = subscribers
