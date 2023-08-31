import json

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.monitor.alerts.alert import Alert
from elementary.monitor.alerts.report_link_utils import get_model_runs_link
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class ModelAlert(Alert):
    TABLE_NAME = "alerts_models"

    def __init__(
        self,
        model_unique_id: str,
        alias: str,
        path: str,
        original_path: str,
        materialization: str,
        message: str,
        full_refresh: bool,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.model_unique_id = model_unique_id
        self.alias = alias
        self.path = path
        self.original_path = original_path
        self.materialization = materialization
        self.message = message
        self.full_refresh = full_refresh
        self.alerts_table = ModelAlert.TABLE_NAME

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
            self.slack_message_builder.create_header_block(f"{icon} dbt model alert")
        ]
        if self.alert_suppression_interval:
            title.extend(
                [
                    self.slack_message_builder.create_context_block(
                        [
                            f"*Model:* {self.alias}     |",
                            f"*Status:* {self.status}",
                        ],
                    ),
                    self.slack_message_builder.create_context_block(
                        [
                            f"*Time:* {self.detected_at_str}     |",
                            f"*Suppression interval:* {self.alert_suppression_interval} hours",
                        ],
                    ),
                ]
            )
        else:
            title.append(
                self.slack_message_builder.create_context_block(
                    [
                        f"*Model:* {self.alias}     |",
                        f"*Status:* {self.status}     |",
                        f"*{self.detected_at_str}*",
                    ],
                ),
            )

        model_runs_report_link = get_model_runs_link(
            self.report_url, self.model_unique_id
        )
        if model_runs_report_link:
            report_link = self.slack_message_builder.create_context_block(
                [
                    f"<{model_runs_report_link.url}|{model_runs_report_link.text}>",
                ],
            )
            title.append(report_link)

        preview = self.slack_message_builder.create_compacted_sections_blocks(
            [
                f"*Tags*\n{tags or '_No tags_'}",
                f"*Owners*\n{owners or '_No owners_'}",
                f"*Subscribers*\n{subscribers or '_No subscribers_'}",
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
                self.slack_message_builder.create_context_block(["*Materialization*"])
            )
            configuration.append(
                self.slack_message_builder.create_text_section_block(
                    f"`{str(self.materialization)}`"
                )
            )
        if self.full_refresh:
            configuration.append(
                self.slack_message_builder.create_context_block(["*Full refresh*"])
            )
            configuration.append(
                self.slack_message_builder.create_text_section_block(
                    f"`{self.full_refresh}`"
                )
            )
        if self.path:
            configuration.append(
                self.slack_message_builder.create_context_block(["*Path*"])
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
            self.slack_message_builder.create_header_block(f"{icon} dbt snapshot alert")
        ]
        if self.alert_suppression_interval:
            title.extend(
                [
                    self.slack_message_builder.create_context_block(
                        [
                            f"*Snapshot:* {self.alias}     |",
                            f"*Status:* {self.status}",
                        ],
                    ),
                    self.slack_message_builder.create_context_block(
                        [
                            f"*Time:* {self.detected_at_str}     |",
                            f"*Suppression interval:* {self.alert_suppression_interval} hours",
                        ],
                    ),
                ]
            )
        else:
            title.append(
                self.slack_message_builder.create_context_block(
                    [
                        f"*Snapshot:* {self.alias}     |",
                        f"*Status:* {self.status}     |",
                        f"*{self.detected_at_str}*",
                    ],
                ),
            )

        model_runs_report_link = get_model_runs_link(
            self.report_url, self.model_unique_id
        )
        if model_runs_report_link:
            report_link = self.slack_message_builder.create_context_block(
                [
                    f"<{model_runs_report_link.url}|{model_runs_report_link.text}>",
                ],
            )
            title.append(report_link)

        preview = self.slack_message_builder.create_compacted_sections_blocks(
            [
                f"*Tags*\n{tags or '_No tags_'}",
                f"*Owners*\n{owners or '_No owners_'}",
                f"*Subscribers*\n{subscribers or '_No subscribers_'}",
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
                self.slack_message_builder.create_context_block(["*Path*"])
            )
            configuration.append(
                self.slack_message_builder.create_text_section_block(
                    f"`{self.original_path}`"
                )
            )

        return self.slack_message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )

    @property
    def concise_name(self):
        if self.materialization == "snapshot":
            text = "snapshot"
        else:
            text = "model"
        return f"dbt {text} alert - {self.alias}"
