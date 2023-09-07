from typing import Any, Dict, List, Optional, TypeVar, Union

from dateutil import tz

from elementary.clients.slack.schema import SlackBlocksType, SlackMessageSchema
from elementary.clients.slack.slack_message_builder import SlackMessageBuilder
from elementary.monitor.alerts.schema.alert import AlertSuppressionSchema
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger
from elementary.utils.time import DATETIME_FORMAT, convert_utc_iso_format_to_datetime

logger = get_logger(__name__)


class Alert:
    def __init__(
        self,
        id: str,
        alert_class_id: Optional[str] = None,
        suppression_status: Optional[str] = None,
        sent_at: Optional[str] = None,
        detected_at: Optional[str] = None,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        elementary_database_and_schema: Optional[str] = None,
        owners: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        status: Optional[str] = None,
        subscribers: Optional[List[str]] = None,
        slack_channel: Optional[str] = None,
        alert_suppression_interval: int = 0,
        alert_fields: Optional[list] = None,
        timezone: Optional[str] = None,
        test_meta: Optional[dict] = None,
        model_meta: Optional[Union[str, dict]] = None,
        alerts_table: Optional[str] = None,
        slack_group_alerts_by: Optional[str] = None,
        report_url: Optional[str] = None,
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
        if detected_at is not None:
            try:
                detected_at_datetime = convert_utc_iso_format_to_datetime(detected_at)
                self.detected_at_utc = detected_at_datetime
                self.detected_at = detected_at_datetime.astimezone(
                    tz.gettz(timezone) if timezone else tz.tzlocal()
                )
            except Exception:
                logger.error('Failed to parse "detected_at" field.')
        self.detected_at_str = (
            self.detected_at.strftime(DATETIME_FORMAT) if self.detected_at else "N/A"
        )
        self.database_name = database_name
        self.schema_name = schema_name
        self.owners: List[str] = owners or []
        self.tags: List[str] = tags or []
        self.meta = test_meta
        self.model_meta = try_load_json(model_meta) or {}
        self.status = status
        self.subscribers: List[str] = subscribers or []
        self.slack_channel = slack_channel
        self.alert_suppression_interval = alert_suppression_interval
        self.alert_fields = alert_fields
        self.slack_group_alerts_by = slack_group_alerts_by
        self.report_url = report_url

        # Defined in the base class so type checks will not complain
        self.data: Dict[str, Any] = {}
        self.model_unique_id: Optional[str] = None
        self.alerts_table = alerts_table

    _LONGEST_MARKDOWN_SUFFIX_LEN = 3
    _CONTINUATION_SYMBOL = "..."

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        raise NotImplementedError

    @property
    def concise_name(self):
        return "Alert"


AlertType = TypeVar("AlertType", bound=Alert)


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
