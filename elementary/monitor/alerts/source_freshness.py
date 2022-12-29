import datetime
from typing import Optional

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.monitor.alerts.alert import Alert
from elementary.utils.log import get_logger
from elementary.utils.time import (
    DATETIME_FORMAT,
    convert_datetime_utc_str_to_timezone_str,
)

logger = get_logger(__name__)


class SourceFreshnessAlert(Alert):
    TABLE_NAME = "alerts_source_freshness"

    def __init__(
        self,
        unique_id: str,
        snapshotted_at: Optional[str],
        max_loaded_at: Optional[str],
        max_loaded_at_time_ago_in_s: Optional[float],
        source_name: str,
        identifier: str,
        freshness_error_after: str,
        freshness_warn_after: str,
        freshness_filter: str,
        path: str,
        error: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.unique_id = unique_id
        self.snapshotted_at = (
            convert_datetime_utc_str_to_timezone_str(snapshotted_at, self.timezone)
            if snapshotted_at
            else None
        )
        self.max_loaded_at = (
            convert_datetime_utc_str_to_timezone_str(max_loaded_at, self.timezone)
            if max_loaded_at
            else None
        )
        self.max_loaded_at_time_ago_in_s = max_loaded_at_time_ago_in_s
        self.source_name = source_name
        self.identifier = identifier
        self.freshness_error_after = freshness_error_after
        self.freshness_warn_after = freshness_warn_after
        self.freshness_filter = freshness_filter
        self.path = path
        self.error = error

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        tags = self.slack_message_builder.prettify_and_dedup_list(self.tags)
        owners = self.slack_message_builder.prettify_and_dedup_list(self.owners)
        subscribers = self.slack_message_builder.prettify_and_dedup_list(
            self.subscribers
        )
        icon = self.slack_message_builder.get_slack_status_icon(self.status)

        title = [
            self.slack_message_builder.create_header_block(
                f"{icon} dbt source freshness alert"
            ),
            self.slack_message_builder.create_context_block(
                [
                    f"*Source:* {self.source_name}.{self.identifier}     |",
                    f"*Status:* {self.status}     |",
                    f"*{self.detected_at.strftime(DATETIME_FORMAT)}*",
                ],
            ),
        ]

        preview = self.slack_message_builder.create_compacted_sections_blocks(
            [
                f"*Tags*\n{tags if tags else '_No tags_'}",
                f"*Owners*\n{owners if owners else '_No owners_'}",
                f"*Subscribers*\n{subscribers if subscribers else '_No subscribers_'}",
            ]
        )

        result = []
        if self.status == "runtime error":
            result.extend(
                [
                    self.slack_message_builder.create_context_block(
                        ["*Result message*"]
                    ),
                    self.slack_message_builder.create_text_section_block(
                        f"Failed to calculate the source freshness\n"
                        f"```{self.error}```"
                    ),
                ]
            )
        else:
            result.extend(
                self.slack_message_builder.create_compacted_sections_blocks(
                    [
                        f"*Time Elapsed*\n{datetime.timedelta(seconds=self.max_loaded_at_time_ago_in_s)}",
                        f"*Last Record At*\n{self.max_loaded_at}",
                        f"*Sampled At*\n{self.snapshotted_at}",
                    ]
                )
            )

        configuration = []
        if self.freshness_error_after:
            configuration.append(
                self.slack_message_builder.create_context_block([f"*Error after*"])
            )
            configuration.append(
                self.slack_message_builder.create_text_section_block(
                    f"`{self.freshness_error_after}`"
                )
            )
        if self.freshness_warn_after:
            configuration.append(
                self.slack_message_builder.create_context_block([f"*Warn after*"])
            )
            configuration.append(
                self.slack_message_builder.create_text_section_block(
                    f"`{self.freshness_warn_after}`"
                )
            )
        if self.freshness_filter:
            configuration.append(
                self.slack_message_builder.create_context_block([f"*Filter*"])
            )
            configuration.append(
                self.slack_message_builder.create_text_section_block(
                    f"`{self.freshness_filter}`"
                )
            )
        if self.path:
            configuration.append(
                self.slack_message_builder.create_context_block([f"*Path*"])
            )
            configuration.append(
                self.slack_message_builder.create_text_section_block(f"`{self.path}`")
            )

        return self.slack_message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )
