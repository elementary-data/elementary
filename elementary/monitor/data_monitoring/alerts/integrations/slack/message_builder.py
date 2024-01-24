from typing import Optional

from elementary.clients.slack.schema import SlackBlocksType, SlackMessageSchema
from elementary.clients.slack.slack_message_builder import SlackMessageBuilder


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
