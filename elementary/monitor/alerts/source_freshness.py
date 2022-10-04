import datetime
from typing import Optional

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.monitor.alerts.alert import Alert
from elementary.utils.log import get_logger
from elementary.utils.time import try_parse_time

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
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.unique_id = unique_id
        self.snapshotted_at = try_parse_time(snapshotted_at) if snapshotted_at else None
        self.max_loaded_at = try_parse_time(max_loaded_at) if max_loaded_at else None
        self.max_loaded_at_time_ago_in_s = max_loaded_at_time_ago_in_s
        self.source_name = source_name
        self.identifier = identifier
        self.freshness_error_after = freshness_error_after
        self.freshness_warn_after = freshness_warn_after
        self.freshness_filter = freshness_filter
        self.path = path

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        icon = ":small_red_triangle:"
        if self.status == "warn":
            icon = ":warning:"
        elif self.status == "runtime error":
            icon = ":no_entry:"
        slack_message = {"attachments": [{"blocks": []}]}
        self._add_text_section_to_slack_msg(
            slack_message, f"{icon} *dbt source freshness alert*"
        )
        self._add_fields_section_to_slack_msg(
            slack_message,
            [f"*Source*\n{self.identifier}", f"*When*\n{self.detected_at}"],
            divider=True,
        )
        if self.status == "error":
            self._add_fields_section_to_slack_msg(
                slack_message,
                [
                    f"*Time Elapsed*\n{datetime.timedelta(seconds=self.max_loaded_at_time_ago_in_s)}"
                ],
            )
            self._add_fields_section_to_slack_msg(
                slack_message,
                [
                    f"*Last Record At*\n{self.max_loaded_at}",
                    f"*Sampled At*\n{self.snapshotted_at}",
                ],
                divider=True,
            )
        elif self.status == "runtime error":
            self._add_text_section_to_slack_msg(
                slack_message,
                f"*Error Message*\nFailed to calculate the source freshness.",
            )
        self._add_fields_section_to_slack_msg(
            slack_message,
            [
                f"*Error After*\n`{self.freshness_error_after}`",
                f"*Warn After*\n`{self.freshness_warn_after}`",
            ],
        )
        if self.freshness_filter:
            self._add_text_section_to_slack_msg(
                slack_message, f"*Filter*\n`{self.freshness_filter}`"
            )

        self._add_fields_section_to_slack_msg(
            slack_message, [f"*Owners*\n{self.owners}", f"*Tags*\n{self.tags}"]
        )
        if self.subscribers:
            self._add_fields_section_to_slack_msg(
                slack_message, [f'*Subscribers*\n{", ".join(set(self.subscribers))}']
            )
        self._add_fields_section_to_slack_msg(
            slack_message, [f"*Status*\n{self.status}", f"*Path*\n{self.path}"]
        )
        return SlackMessageSchema(attachments=slack_message["attachments"])
