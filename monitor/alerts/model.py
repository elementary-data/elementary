import json
from dataclasses import dataclass
from datetime import datetime
from typing import List, Union

from clients.slack.schema import SlackMessageSchema
from monitor.alerts.alert import Alert
from utils.json_utils import prettify_json_str_set


@dataclass
class ModelAlert(Alert):
    unique_id: str
    alias: str
    path: str
    materialization: str
    detected_at: Union[str, datetime]
    database_name: str
    schema_name: str
    message: str
    full_refresh: bool
    owners: str
    tags: str
    status: str
    subscribers: List[str]

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
        self._add_fields_section_to_slack_msg(slack_message, [f'*Owners*\n{self.owners}', f'*Subscribers*\n{", ".join(set(self.subscribers))}' f'*Tags*\n{self.tags}'])
        self._add_text_section_to_slack_msg(slack_message, f'*Error Message*\n```{self.message}```')
        self._add_fields_section_to_slack_msg(slack_message,
                                              [f'*Full Refresh*\n{self.full_refresh}', f'*Path*\n{self.path}'],
                                              divider=True)
        self._add_fields_section_to_slack_msg(slack_message, [f'*Status*\n{self.status}',
                                                              f'*Materialization*\n{self.materialization}'])
        return SlackMessageSchema(attachments=slack_message['attachments'])
