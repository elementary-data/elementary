import json

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.monitor.alerts.alert import Alert
from elementary.utils.log import get_logger
from elementary.utils.time import DATETIME_FORMAT

logger = get_logger(__name__)


class ModelAlert(Alert):
    TABLE_NAME = "alerts_models"

    def __init__(
        self,
        unique_id: str,
        alias: str,
        path: str,
        original_path: str,
        materialization: str,
        message: str,
        full_refresh: bool,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.unique_id = unique_id
        self.alias = alias
        self.path = path
        self.original_path = original_path
        self.materialization = materialization
        self.message = message
        self.full_refresh = full_refresh

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        if is_slack_workflow:
            return SlackMessageSchema(text=json.dumps(self.__dict__))
        if self.materialization == "snapshot":
            return self._snapshot_to_slack()
        return self._model_to_slack()

    def _model_to_slack(self):
        icon = ":small_red_triangle:"
        if self.status == "warn":
            icon = ":warning:"
        slack_message = {"attachments": [{"blocks": []}]}
        self._add_text_section_to_slack_msg(slack_message, f"{icon} *dbt model alert*")
        self._add_fields_section_to_slack_msg(
            slack_message,
            [
                f"*Model*\n{self.alias}",
                f"*When*\n{self.detected_at.strftime(DATETIME_FORMAT)}",
            ],
            divider=True,
        )
        self._add_fields_section_to_slack_msg(
            slack_message,
            [f"*Status*\n{self.status}", f"*Materialization*\n{self.materialization}"],
        )
        self._add_fields_section_to_slack_msg(
            slack_message, [f"*Owners*\n{self.owners}", f"*Tags*\n{self.tags}"]
        )
        if self.subscribers:
            self._add_fields_section_to_slack_msg(
                slack_message, [f'*Subscribers*\n{", ".join(set(self.subscribers))}']
            )
        self._add_fields_section_to_slack_msg(
            slack_message,
            [f"*Full Refresh*\n{self.full_refresh}", f"*Path*\n{self.path}"],
            divider=True,
        )
        if self.message:
            self._add_text_section_to_slack_msg(
                slack_message, f"*Error Message*\n```{self.message}```"
            )
        return SlackMessageSchema(attachments=slack_message["attachments"])

    def _snapshot_to_slack(self):
        icon = ":small_red_triangle:"
        if self.status == "warn":
            icon = ":warning:"
        slack_message = {"attachments": [{"blocks": []}]}
        self._add_text_section_to_slack_msg(
            slack_message, f"{icon} *dbt snapshot alert*"
        )
        self._add_fields_section_to_slack_msg(
            slack_message,
            [
                f"*Snapshot*\n{self.alias}",
                f"*When*\n{self.detected_at.strftime(DATETIME_FORMAT)}",
            ],
            divider=True,
        )
        self._add_fields_section_to_slack_msg(
            slack_message, [f"*Owners*\n{self.owners}", f"*Tags*\n{self.tags}"]
        )
        if self.subscribers:
            self._add_fields_section_to_slack_msg(
                slack_message, [f'*Subscribers*\n{", ".join(set(self.subscribers))}']
            )
        self._add_fields_section_to_slack_msg(
            slack_message, [f"*Status*\n{self.status}", f"*Path*\n{self.original_path}"]
        )
        self._add_text_section_to_slack_msg(
            slack_message, f"*Error Message*\n```{self.message}```"
        )
        return SlackMessageSchema(attachments=slack_message["attachments"])
