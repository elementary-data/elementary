from elementary.clients.slack.schema import SlackMessageSchema
from elementary.monitor.alerts.alert import Alert
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class SourceFreshnessAlert(Alert):
    TABLE_NAME = "alerts_source_freshness"

    def __init__(self, unique_id: str, max_loaded_at: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.unique_id = unique_id
        self.max_loaded_at = max_loaded_at

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        icon = ":small_red_triangle:"
        if self.status == "warn":
            icon = ":warning:"
        slack_message = {"attachments": [{"blocks": []}]}
        self._add_text_section_to_slack_msg(
            slack_message, f"{icon} *dbt source freshness alert*"
        )
        self._add_fields_section_to_slack_msg(
            slack_message,
            [f"*Source*\n{self.unique_id}", f"*When*\n{self.detected_at}"],
            divider=True,
        )
        self._add_fields_section_to_slack_msg(
            slack_message, [f"*Status*\n{self.status}"]
        )
        self._add_fields_section_to_slack_msg(
            slack_message, [f"*Owners*\n{self.owners}", f"*Tags*\n{self.tags}"]
        )
        if self.subscribers:
            self._add_fields_section_to_slack_msg(
                slack_message, [f'*Subscribers*\n{", ".join(set(self.subscribers))}']
            )
            self._add_text_section_to_slack_msg(
                slack_message, f"*Max Loaded At*\n{self.max_loaded_at}"
            )
        return SlackMessageSchema(attachments=slack_message["attachments"])
