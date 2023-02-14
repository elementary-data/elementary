from typing import List, Optional

from dateutil import tz

from elementary.clients.slack.schema import SlackBlocksType, SlackMessageSchema
from elementary.clients.slack.slack_message_builder import SlackMessageBuilder
from elementary.monitor.alerts.schema.alert import AlertSuppressionSchema
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger
from elementary.utils.time import convert_utc_iso_format_to_datetime

logger = get_logger(__name__)


class Alert:
    def __init__(
        self,
        id: str,
        alert_class_id: str = None,
        suppression_status: str = None,
        sent_at: str = None,
        detected_at: str = None,
        database_name: str = None,
        schema_name: str = None,
        elementary_database_and_schema: str = None,
        owners: str = None,
        tags: str = None,
        status: str = None,
        subscribers: Optional[List[str]] = None,
        slack_channel: Optional[str] = None,
        alert_suppression_interval: int = 0,
        alert_fields: Optional[list] = None,
        timezone: Optional[str] = None,
        meta: Optional[dict] = None,
        model_meta: Optional[str] = None,
        **kwargs,
    ):
        self.slack_message_builder = SlackAlertMessageBuilder()
        self.id = id
        self.alert_class_id = alert_class_id
        self.alert_suppression = AlertSuppressionSchema(
            suppression_status=suppression_status,
            sent_at=sent_at,
        )
        self.elementary_database_and_schema = elementary_database_and_schema
        self.detected_at_utc = None
        self.detected_at = None
        self.timezone = timezone
        try:
            detected_at_datetime = convert_utc_iso_format_to_datetime(detected_at)
            self.detected_at_utc = detected_at_datetime
            self.detected_at = detected_at_datetime.astimezone(
                tz.gettz(timezone) if timezone else tz.tzlocal()
            )
        except Exception:
            logger.error('Failed to parse "detected_at" field.')
        self.database_name = database_name
        self.schema_name = schema_name
        self.owners = owners
        self.tags = tags
        self.meta = meta
        self.model_meta = try_load_json(model_meta) or {}
        self.status = status
        self.subscribers = subscribers
        self.slack_channel = slack_channel
        self.alert_suppression_interval = alert_suppression_interval
        self.alert_fields = alert_fields

    _LONGEST_MARKDOWN_SUFFIX_LEN = 3
    _CONTINUATION_SYMBOL = "..."

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        raise NotImplementedError


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
        return f"{len(self.preview_blocks)} provieded -> {self.message}"


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
        self._add_title_to_slack_alert(title)
        self._add_preview_to_slack_alert(preview)
        self._add_details_to_slack_alert(result, configuration)
        return super().get_slack_message()

    def _add_title_to_slack_alert(self, title_blocks: Optional[SlackBlocksType] = None):
        if title_blocks:
            title = [*title_blocks, self.create_divider_block()]
            self._add_always_displayed_blocks(title)

    def _add_preview_to_slack_alert(
        self, preview_blocks: Optional[SlackBlocksType] = None
    ):
        if preview_blocks:
            validated_preview_blocks = self._validate_preview_blocks(preview_blocks)
            self._add_blocks_as_attachments(validated_preview_blocks)

    def _add_details_to_slack_alert(
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

    @classmethod
    def _validate_preview_blocks(cls, preview_blocks: Optional[SlackBlocksType] = None):
        if not preview_blocks:
            return

        preview_blocks_count = len(preview_blocks)
        if preview_blocks_count > SlackMessageBuilder._MAX_ALERT_PREVIEW_BLOCKS:
            raise PreviewIsTooLongError(preview_blocks)

        elif (
            preview_blocks_count < SlackMessageBuilder._MAX_ALERT_PREVIEW_BLOCKS
            and preview_blocks_count > 0
        ):
            padded_preview_blocks = [*preview_blocks]
            blocks_counter = preview_blocks_count
            while blocks_counter < SlackMessageBuilder._MAX_ALERT_PREVIEW_BLOCKS:
                padded_preview_blocks.append(cls.create_empty_section_block())
                blocks_counter += 1
            return padded_preview_blocks

        else:
            return preview_blocks
