import json
from typing import Any

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.monitor.alerts.alert import Alert


class MalformedAlert(Alert):
    def __init__(self, data: dict, **kwargs):
        super().__init__(**kwargs)
        self.data = data

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        return SlackMessageSchema(
            text=self.slack_message_builder.get_limited_markdown_msg(
                f":small_red_triangle: Oops, we failed to format the alert :confused:\n"
                f"Please share this with the Elementary team via <https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg|Slack> or a <https://github.com/elementary-data/elementary/issues/new|GitHub> issue.\n"
                f"```{json.dumps(self.data, indent=2)}```"
            )
        )

    # We use getattribute and not getattr because "Alert" set some attributes to None if not given which skip the
    # self.data.get(_name) For example - tags is set to None, so getattr for tags will return None although data
    # contains it.
    def __getattribute__(self, __name: str) -> Any:
        try:
            res = super().__getattribute__(__name)
        except AttributeError:
            res = None

        if not res:
            # Try to get from self.data, but also safely handle the case it doesn't
            # exist yet without infinite recursion.
            try:
                data = super().__getattribute__("data")
                res = data.get(__name)
            except AttributeError:
                pass
        return res
