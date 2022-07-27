import json
from datetime import datetime
from typing import List, Optional, Union

from clients.slack.schema import SlackMessageSchema
from monitor.alerts.alert import Alert
from utils.json_utils import prettify_json_str_set
from utils.log import get_logger
from utils.time import convert_utc_time_to_local_time

logger = get_logger(__name__)


class ModelAlert(Alert):
    def __init__(
            self,
            id: str,
            elementary_database_and_schema: str,
            unique_id: str,
            alias: str,
            path: str,
            original_path: str,
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
        self.original_path = original_path
        self.materialization = materialization
        self.detected_at_utc = None
        self.detected_at = None
        if detected_at:
            try:
                detected_at_utc = datetime.fromisoformat(detected_at)
                self.detected_at_utc = detected_at_utc.strftime('%Y-%m-%d %H:%M:%S')
                self.detected_at = convert_utc_time_to_local_time(detected_at_utc).strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                logger.error(f'Failed to parse "detect_at" field.')
        self.database_name = database_name
        self.schema_name = schema_name
        self.message = message
        self.full_refresh = full_refresh
        self.owners = prettify_json_str_set(owners)
        self.tags = prettify_json_str_set(tags)
        self.status = status

    TABLE_NAME = 'alerts_models'

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        if is_slack_workflow:
            return SlackMessageSchema(text=json.dumps(self.__dict__))
        if self.materialization == 'snapshot':
            return self._snapshot_to_slack()
        return self._model_to_slack()

    def _model_to_slack(self):
        icon = ':small_red_triangle:'
        if self.status == 'warn':
            icon = ':warning:'
        slack_message = {'attachments': [{'blocks': []}]}
        self._add_text_section_to_slack_msg(slack_message, f'{icon} *dbt model alert*')
        self._add_fields_section_to_slack_msg(slack_message,
                                              [f'*Model*\n{self.alias}',
                                               f'*When*\n{self.detected_at}'],
                                              divider=True)
        self._add_fields_section_to_slack_msg(slack_message, [f'*Status*\n{self.status}',
                                                              f'*Materialization*\n{self.materialization}'])
        self._add_fields_section_to_slack_msg(slack_message, [f'*Owners*\n{self.owners}', f'*Tags*\n{self.tags}'])
        if self.subscribers:
            self._add_fields_section_to_slack_msg(slack_message, [f'*Subscribers*\n{", ".join(set(self.subscribers))}'])
        self._add_fields_section_to_slack_msg(slack_message,
                                              [f'*Full Refresh*\n{self.full_refresh}', f'*Path*\n{self.path}'],
                                              divider=True)
        self._add_text_section_to_slack_msg(slack_message, f'*Error Message*\n```{self.message}```')
        return SlackMessageSchema(attachments=slack_message['attachments'])

    def _snapshot_to_slack(self):
        icon = ':small_red_triangle:'
        if self.status == 'warn':
            icon = ':warning:'
        slack_message = {'attachments': [{'blocks': []}]}
        self._add_text_section_to_slack_msg(slack_message, f'{icon} *dbt snapshot alert*')
        self._add_fields_section_to_slack_msg(slack_message,
                                              [f'*Snapshot*\n{self.alias}',
                                               f'*When*\n{self.detected_at}'],
                                              divider=True)
        self._add_fields_section_to_slack_msg(slack_message, [f'*Owners*\n{self.owners}', f'*Tags*\n{self.tags}'])
        if self.subscribers:
            self._add_fields_section_to_slack_msg(slack_message, [f'*Subscribers*\n{", ".join(set(self.subscribers))}'])
        self._add_fields_section_to_slack_msg(slack_message,
                                              [f'*Status*\n{self.status}', f'*Path*\n{self.original_path}'])
        self._add_text_section_to_slack_msg(slack_message, f'*Error Message*\n```{self.message}```')
        return SlackMessageSchema(attachments=slack_message['attachments'])
