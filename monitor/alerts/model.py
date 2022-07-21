import json
from datetime import datetime
from typing import List, Optional, Union

from clients.slack.schema import SlackMessageSchema
from monitor.alerts.alert import Alert
from utils.json_utils import prettify_json_str_set


class ModelAlert(Alert):
    def __init__(
        self,
        id: str,
        elementary_database_and_schema: str,
        unique_id: str,
        alias: str,
        path: str,
        materialization: str,
        detected_at: Union[str, datetime],
        database_name: str,
        schema_name: str,
        message: str,
        full_refresh: bool,
        owners: str,
        tags: str,
        status: str,
        subscribers: Optional[List[str]] = None,
        slack_channel: Optional[str] = None,
        **kwargs
    ) -> None:
        super().__init__(id, elementary_database_and_schema, subscribers, slack_channel)
        self.unique_id = unique_id
        self.alias = alias
        self.path = path
        self.materialization = materialization
        self.detected_at = detected_at
        self.database_name = database_name
        self.schema_name = schema_name
        self.message = message
        self.full_refresh = full_refresh
        self.owners = owners
        self.tags = tags
        self.status = status

    TABLE_NAME = 'alerts_models'

    def __post_init__(self):
        self.owners = prettify_json_str_set(self.owners)
        self.tags = prettify_json_str_set(self.tags) 

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        icon = ':small_red_triangle:'
        if self.status == 'warn':
            icon = ':warning:'
        if is_slack_workflow:
            return SlackMessageSchema(text=json.dumps(self.__dict__))
        slack_message = {'attachments': [{'blocks': []}]}
        self._add_text_section_to_slack_msg(slack_message, f'{icon} *dbt model alert*')
        self._add_fields_section_to_slack_msg(slack_message,
                                              [f'*Model*\n{self.alias}',
                                               f'*When*\n{self.detected_at}'],
                                              divider=True)
        self._add_fields_section_to_slack_msg(slack_message,
                                              [f'*Owners*\n{self.owners}', f'*Tags*\n{self.tags}'])
        if self.subscribers:
            self._add_fields_section_to_slack_msg(slack_message, [f'*Subscribers*\n{", ".join(set(self.subscribers))}'])
        self._add_text_section_to_slack_msg(slack_message, f'*Error Message*\n```{self.message}```')
        self._add_fields_section_to_slack_msg(slack_message,
                                              [f'*Full Refresh*\n{self.full_refresh}', f'*Path*\n{self.path}'],
                                              divider=True)
        self._add_fields_section_to_slack_msg(slack_message, [f'*Status*\n{self.status}',
                                                              f'*Materialization*\n{self.materialization}'])
        return SlackMessageSchema(attachments=slack_message['attachments'])
