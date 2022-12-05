import json

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.monitor.alerts.alert import Alert
from elementary.utils.json_utils import prettify_json_str_set
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
        icon = self._get_slack_status_icon()
        slack_message = self._initial_slack_message()

        # Alert info section
        self._add_header_to_slack_msg(slack_message, f"{icon} dbt model alert")
        self._add_context_to_slack_msg(
            slack_message,
            [
                f"*Model:* {self.alias}     |",
                f"*Status:* {self.status}     |",
                f"*{self.detected_at.strftime(DATETIME_FORMAT)}*",
            ],
        )
        self._add_divider(slack_message)

        compacted_sections = [
            f"*Materialization*\n{self.materialization if self.materialization else '_No materialization_'}",
            f"*Tags*\n{self.tags if self.tags else '_No tags_'}",
            f"*Owners*\n{self.owners if self.owners else '_No owners_'}",
            f"*Subscribers*\n{', '.join(set(self.subscribers)) if self.subscribers else '_No subscribers_'}",
        ]
        self._add_compacted_sections_to_slack_msg(
            slack_message, compacted_sections, add_to_attachment=True
        )

        # Pad till "See more"
        self._add_empty_section_to_slack_msg(slack_message, add_to_attachment=True)
        self._add_empty_section_to_slack_msg(slack_message, add_to_attachment=True)
        self._add_empty_section_to_slack_msg(slack_message, add_to_attachment=True)

        # Result sectiom
        if self.message:
            self._add_text_section_to_slack_msg(
                slack_message, f":mag: *Run*", add_to_attachment=True
            )
            self._add_divider(slack_message, add_to_attachment=True)

            self._add_context_to_slack_msg(
                slack_message, [f"*Run message*"], add_to_attachment=True
            )
            self._add_text_section_to_slack_msg(
                slack_message,
                f"```{self.message.strip()}```",
                add_to_attachment=True,
            )

        # Configuration section
        if self.full_refresh or self.path:
            self._add_text_section_to_slack_msg(
                slack_message,
                f":hammer_and_wrench: *Configuration*",
                add_to_attachment=True,
            )
            self._add_divider(slack_message, add_to_attachment=True)

            if self.full_refresh:
                self._add_context_to_slack_msg(
                    slack_message, [f"*Full refresh*"], add_to_attachment=True
                )
                self._add_text_section_to_slack_msg(
                    slack_message,
                    f"`{str(self.full_refresh)}`",
                    add_to_attachment=True,
                )

            if self.path:
                self._add_context_to_slack_msg(
                    slack_message, [f"*Path*"], add_to_attachment=True
                )
                self._add_text_section_to_slack_msg(
                    slack_message,
                    f"`{self.path}`",
                    add_to_attachment=True,
                )
        return SlackMessageSchema(**slack_message)

    def _snapshot_to_slack(self):
        icon = self._get_slack_status_icon()
        slack_message = self._initial_slack_message()

        # Alert info section
        self._add_header_to_slack_msg(slack_message, f"{icon} dbt snapshot alert")
        self._add_context_to_slack_msg(
            slack_message,
            [
                f"*Snapshot:* {self.alias}     |",
                f"*Status:* {self.status}     |",
                f"*{self.detected_at.strftime(DATETIME_FORMAT)}*",
            ],
        )
        self._add_divider(slack_message)

        compacted_sections = [
            f"*Tags*\n{prettify_json_str_set(self.tags) if self.tags else '_No tags_'}",
            f"*Owners*\n{prettify_json_str_set(self.owners) if self.owners else '_No owners_'}",
            f"*Subscribers*\n{prettify_json_str_set(self.subscribers) if self.subscribers else '_No subscribers_'}",
        ]
        self._add_compacted_sections_to_slack_msg(
            slack_message, compacted_sections, add_to_attachment=True
        )

        # Pad till "See more"
        self._add_empty_section_to_slack_msg(slack_message, add_to_attachment=True)
        self._add_empty_section_to_slack_msg(slack_message, add_to_attachment=True)
        self._add_empty_section_to_slack_msg(slack_message, add_to_attachment=True)

        # Result sectiom
        if self.message:
            self._add_text_section_to_slack_msg(
                slack_message, f":mag: *Run*", add_to_attachment=True
            )
            self._add_divider(slack_message, add_to_attachment=True)

            self._add_context_to_slack_msg(
                slack_message, [f"*Run message*"], add_to_attachment=True
            )
            self._add_text_section_to_slack_msg(
                slack_message,
                f"```{self.message.strip()}```",
                add_to_attachment=True,
            )

        # Configuration section
        if self.original_path:
            self._add_text_section_to_slack_msg(
                slack_message,
                f":hammer_and_wrench: *Configuration*",
                add_to_attachment=True,
            )
            self._add_divider(slack_message, add_to_attachment=True)

            self._add_context_to_slack_msg(
                slack_message, [f"*Path*"], add_to_attachment=True
            )
            self._add_text_section_to_slack_msg(
                slack_message,
                f"`{self.original_path}`",
                add_to_attachment=True,
            )
        return SlackMessageSchema(**slack_message)
