from typing import Optional, Union

from elementary.clients.slack.schema import SlackBlocksType, SlackMessageSchema
from elementary.clients.slack.slack_message_builder import SlackMessageBuilder
from elementary.monitor.alerts.alert import (
    DEFAULT_ALERT_FIELDS,
    STATUS_DISPLAYS,
    AlertModel,
)
from elementary.monitor.alerts.group_of_alerts import GroupedByTableAlerts
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel


class PreviewIsTooLongError(Exception):
    def __init__(
        self,
        preview_blocks: SlackBlocksType,
        message: str = f"There are too many blocks at the preview section of the alert (more than {SlackMessageBuilder._MAX_ALERT_PREVIEW_BLOCKS})",
    ) -> None:
        self.preview_blocks = preview_blocks
        self.message = message
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{len(self.preview_blocks)} provided -> {self.message}"


class SlackAlertMessageBuilder(SlackMessageBuilder):
    def __init__(self) -> None:
        super().__init__()

    def get_slack_message(
        self,
        title: Optional[SlackBlocksType] = None,
        preview: Optional[SlackBlocksType] = None,
        result: Optional[SlackBlocksType] = None,
        configuration: Optional[SlackBlocksType] = None,
    ) -> SlackMessageSchema:
        return self._create_slack_alert(
            title=title, preview=preview, result=result, configuration=configuration
        )

    def _create_slack_alert(
        self,
        title: Optional[SlackBlocksType] = None,
        preview: Optional[SlackBlocksType] = None,
        result: Optional[SlackBlocksType] = None,
        configuration: Optional[SlackBlocksType] = None,
    ) -> SlackMessageSchema:
        self.add_title_to_slack_alert(title)
        self.add_preview_to_slack_alert(preview)
        self.add_details_to_slack_alert(result, configuration)
        return super().get_slack_message()

    def get_alert_title(self, alert: AlertModel, include_report_link: bool = True):
        if alert.suppression_interval:
            context = (
                " \t|\t ".join(
                    [
                        f"*Time*: {alert.detected_at_str}",
                        f"*Suppression interval:* {alert.suppression_interval} hours",
                    ]
                ),
            )
        else:
            context = f"*Latest test run*: {alert.detected_at_str}"

        if include_report_link:
            report_link = alert.get_report_link()
            if report_link:
                return [
                    self.create_header_block(
                        f"{STATUS_DISPLAYS[alert.status]}: {alert.summary}"
                    ),
                    self.create_section_with_button(context, report_link),
                ]

        return [
            self.create_header_block(
                f"{STATUS_DISPLAYS[alert.status]}: {alert.summary}"
            ),
            self.create_text_section_block(context),
        ]

    def get_compact_sections_for_alert(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
        ],
        content: dict,
    ):
        compacted_sections = []
        for field_name, field_value in content.items():
            if field_name in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
                title = field_name.title().replace("_", " ")
                value = (
                    self.prettify_and_dedup_list(field_value)
                    if field_value
                    else f"_No {title}_"
                )
                compacted_sections.append(
                    self.create_text_section_block(f"*{title}*\n{value}")
                )

        return (
            self.create_compacted_sections_blocks(compacted_sections)
            if compacted_sections
            else []
        )

    def get_extended_sections_for_alert(self, alert: AlertModel, content: dict):
        return [
            [
                self.create_context_block(
                    [f"*{field_name.title().replace('_', ' ')}*"]
                ),
                self.create_text_section_block(f"```{field_text.strip()}```"),
            ]
            for field_name, field_text in content.items()
            if field_name in (alert.alert_fields or DEFAULT_ALERT_FIELDS) and content
        ]

    def get_description_blocks(self, description: Optional[str]):
        if description:
            return (
                [
                    self.create_text_section_block("*Description*"),
                    self.create_context_block([description]),
                ]
                if description
                else [self.create_text_section_block("*Description*\n_No description_")]
            )

    def add_title_to_slack_alert(self, title_blocks: Optional[SlackBlocksType] = None):
        if title_blocks:
            title = [*title_blocks, self.create_divider_block()]
            self._add_always_displayed_blocks(title)

    def add_preview_to_slack_alert(
        self, preview_blocks: Optional[SlackBlocksType] = None
    ):
        if preview_blocks:
            validated_preview_blocks = self._validate_preview_blocks(preview_blocks)
            self._add_blocks_as_attachments(validated_preview_blocks)

    def add_details_to_slack_alert(
        self,
        result: Optional[SlackBlocksType] = None,
        configuration: Optional[SlackBlocksType] = None,
    ):
        if result:
            result_blocks = [
                self.create_text_section_block(":mag: *Result*"),
                self.create_divider_block(),
                *result,
            ]
            self._add_blocks_as_attachments(result_blocks)

        if configuration:
            configuration_blocks = [
                self.create_text_section_block(":hammer_and_wrench: *Configuration*"),
                self.create_divider_block(),
                *configuration,
            ]
            self._add_blocks_as_attachments(configuration_blocks)

    def add_alert_color(self, alert):
        color = STATUS_DISPLAYS[alert.status]["color"]
        self.add_color_to_slack_alert(color)

    def add_color_to_slack_alert(self, color: str):
        for block in self.slack_message.get("blocks", []):
            block["color"] = color
        for attachment in self.slack_message.get("attachments", []):
            attachment["color"] = color

    @classmethod
    def _validate_preview_blocks(cls, preview_blocks: Optional[SlackBlocksType] = None):
        """
        This function -
         0/ For None, returns None. Otherwise :
         1/ makes sure preview_blocks number is not bigger than the max num of blocks set in SlackMessageBuilder
         2/ pads with empty blocks in case there's not enough preview blocks
                                          (we want to control the cutoff, so we need an exact number of preview blocks)
        :param preview_blocks:
        :return:
        """
        if (
            not preview_blocks
        ):  # this condition captures case of Null and also of a list with length 0
            return

        preview_blocks_count = len(preview_blocks)

        if preview_blocks_count > SlackMessageBuilder._MAX_ALERT_PREVIEW_BLOCKS:
            raise PreviewIsTooLongError(preview_blocks)

        if preview_blocks_count == SlackMessageBuilder._MAX_ALERT_PREVIEW_BLOCKS:
            return preview_blocks

        padded_preview_blocks = [*preview_blocks]
        padding_length = (
            SlackMessageBuilder._MAX_ALERT_PREVIEW_BLOCKS - preview_blocks_count
        )
        padding = [cls.create_empty_section_block() for i in range(padding_length)]
        padded_preview_blocks.extend(padding)
        return padded_preview_blocks
