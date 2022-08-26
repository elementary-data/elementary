import json

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.monitor.alerts.alert import Alert


class MalformedAlert(Alert):
    def __init__(self, data: dict, **kwargs):
        super().__init__(**kwargs)
        self.data = data

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        return SlackMessageSchema(
            text=self._format_section_msg(
                f":small_red_triangle: Oops, we failed to format the alert :confused:\n"
                f"Please share this with the Elementary team via <https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg|Slack> or a <https://github.com/elementary-data/elementary/issues/new|GitHub> issue.\n"
                f"```{json.dumps(self.data, indent=2)}```"
            )
        )
