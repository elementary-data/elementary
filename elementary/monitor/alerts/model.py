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
        tags = self.slack_message_builder.prettify_and_dedup_list(self.tags)
        owners = self.slack_message_builder.prettify_and_dedup_list(self.owners)
        subscribers = self.slack_message_builder.prettify_and_dedup_list(
            self.subscribers
        )
        icon = self.slack_message_builder.get_slack_status_icon(self.status)

        title = [
            self.slack_message_builder.create_header_block(f"{icon} dbt model alert"),
            self.slack_message_builder.create_context_block(
                [
                    f"*Model:* {self.alias}     |",
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
        if self.message:
            result.extend(
                [
                    self.slack_message_builder.create_context_block(
                        ["*Result message*"]
                    ),
                    self.slack_message_builder.create_text_section_block(
                        f"```{self.message.strip()}```"
                    ),
                ]
            )

        configuration = []
        if self.materialization:
            configuration.append(
                self.slack_message_builder.create_context_block([f"*Materialization*"])
            )
            configuration.append(
                self.slack_message_builder.create_text_section_block(
                    f"`{str(self.materialization)}`"
                )
            )
        if self.full_refresh:
            configuration.append(
                self.slack_message_builder.create_context_block([f"*Full refresh*"])
            )
            configuration.append(
                self.slack_message_builder.create_text_section_block(
                    f"`{self.full_refresh}`"
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

    def _snapshot_to_slack(self):
        tags = self.slack_message_builder.prettify_and_dedup_list(self.tags)
        owners = self.slack_message_builder.prettify_and_dedup_list(self.owners)
        subscribers = self.slack_message_builder.prettify_and_dedup_list(
            self.subscribers
        )
        icon = self.slack_message_builder.get_slack_status_icon(self.status)

        title = [
            self.slack_message_builder.create_header_block(
                f"{icon} dbt snapshot alert"
            ),
            self.slack_message_builder.create_context_block(
                [
                    f"*Snapshot:* {self.alias}     |",
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
        if self.message:
            result.extend(
                [
                    self.slack_message_builder.create_context_block(
                        ["*Result message*"]
                    ),
                    self.slack_message_builder.create_text_section_block(
                        f"```{self.message.strip()}```"
                    ),
                ]
            )

        configuration = []
        if self.original_path:
            configuration.append(
                self.slack_message_builder.create_context_block([f"*Path*"])
            )
            configuration.append(
                self.slack_message_builder.create_text_section_block(
                    f"`{self.original_path}`"
                )
            )

        return self.slack_message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )
