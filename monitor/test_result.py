import json
from datetime import datetime
import re
from typing import Optional

from slack_sdk.models.blocks import SectionBlock

from clients.slack.schema import SlackMessageSchema
from utils.json_utils import try_load_json
from utils.log import get_logger
from utils.time import convert_utc_time_to_local_time

logger = get_logger(__name__)


class TestResult(object):
    _LONGEST_MARKDOWN_SUFFIX_LEN = 3
    _CONTINUATION_SYMBOL = '...'

    def __init__(
            self,
            id: str,
            model_unique_id: str,
            test_unique_id: str,
            status: str,
    ) -> None:
        self.id = id
        self.model_unique_id = model_unique_id
        self.test_unique_id = test_unique_id
        self.status = status

    @staticmethod
    def create_test_result_from_dict(test_result_dict: dict) -> Optional['TestResult']:
        test_type = test_result_dict.get('test_type')
        try:
            if test_type == 'dbt_test':
                return DbtTestResult(**test_result_dict)
            else:
                return ElementaryTestResult(**test_result_dict)
        except Exception:
            logger.error(f"Failed parsing test result - {json.dumps(test_result_dict)}")
            return None

    def to_slack_message(self, slack_workflows: bool = False) -> dict:
        raise NotImplementedError

    def to_test_result_api_dict(self) -> dict:
        pass

    @classmethod
    def format_section_msg(cls, section_msg):
        if len(section_msg) < SectionBlock.text_max_length:
            return section_msg
        return section_msg[
               :SectionBlock.text_max_length - len(cls._CONTINUATION_SYMBOL) - cls._LONGEST_MARKDOWN_SUFFIX_LEN] + \
               cls._CONTINUATION_SYMBOL + section_msg[-cls._LONGEST_MARKDOWN_SUFFIX_LEN:]

    @classmethod
    def add_fields_section_to_slack_message(cls, slack_message: dict, section_msgs: list, divider: bool = False):
        fields = []
        for section_msg in section_msgs:
            fields.append({
                "type": "mrkdwn",
                "text": cls.format_section_msg(section_msg)
            })

        block = []
        if divider:
            block.append({"type": "divider"})
        block.append({"type": "section", "fields": fields})
        slack_message['attachments'][0]['blocks'].extend(block)

    @classmethod
    def add_text_section_to_slack_message(cls, slack_message: dict, section_msg: str, divider: bool = False):
        block = []
        if divider:
            block.append({"type": "divider"})
        block.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": cls.format_section_msg(section_msg)
            }
        })
        slack_message['attachments'][0]['blocks'].extend(block)

    def generate_slack_message(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        slack_msg = self.to_slack_message()
        return SlackMessageSchema(
            text=json.dumps(slack_msg) if is_slack_workflow else None,
            attachments=slack_msg["attachments"] if not is_slack_workflow else None
        )

    @staticmethod
    def display_name(str_value: str) -> str:
        return ' '.join([word[0].upper() + word[1:] for word in str_value.split('_')])

    @staticmethod
    def description_display_name(description: str, default_description: str = '') -> str:
        if description:
            return description[0].upper() + description[1:]
        else:
            return default_description


class DbtTestResult(TestResult):
    def __init__(
        self,
        id,
        model_unique_id,
        test_unique_id,
        detected_at,
        database_name,
        schema_name,
        table_name,
        column_name,
        test_type,
        test_sub_type,
        test_results_description,
        owners,
        tags,
        test_results_query,
        test_rows_sample,
        other,
        test_name,
        test_params,
        severity,
        status,
        test_runs = None,
        **kwargs
    ) -> None:
        super().__init__(id, model_unique_id, test_unique_id, status)
        self.test_type = test_type
        self.database_name = database_name
        self.schema_name = schema_name
        self.table_name = table_name
        table_full_name_parts = [database_name, schema_name]
        if table_name:
            table_full_name_parts.append(table_name)
        self.table_full_name = '.'.join(table_full_name_parts).lower()
        self.detected_at = None
        self.detected_at_utc = None
        if detected_at:
            try:
                detected_at_utc = datetime.fromisoformat(detected_at)
                self.detected_at_utc = detected_at_utc.strftime('%Y-%m-%d %H:%M:%S')
                self.detected_at = convert_utc_time_to_local_time(detected_at_utc).strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                logger.error(f'Failed to parse "detect_at" field.')
        self.owners = try_load_json(owners) or ''
        if isinstance(self.owners, list):
            self.owners = ', '.join(self.owners)
        self.tags = try_load_json(tags) or ''
        if isinstance(self.tags, list):
            self.tags = ', '.join(self.tags)
        self.test_name = test_name
        self.test_display_name = self.display_name(test_name) if test_name else ''
        self.other = other
        self.test_sub_type = test_sub_type if test_sub_type else ''
        self.test_sub_type_display_name = self.display_name(test_sub_type) if test_sub_type else ''
        self.test_results_query = test_results_query.strip() if test_results_query else ''
        self.test_rows_sample = test_rows_sample if test_rows_sample else ''
        self.test_runs = test_runs if test_runs else ''
        self.test_params = test_params
        self.error_message = self.description_display_name(test_results_description, 'No error message')
        self.column_name = column_name if column_name else ''
        self.severity = severity

        self.failed_rows_count = -1
        if status != 'pass':
            found_rows_number = re.search(r'\d+', self.error_message)
            if found_rows_number:
                found_rows_number = found_rows_number.group()
                self.failed_rows_count = int(found_rows_number)

    def to_slack_message(self, slack_workflows: bool = False) -> dict:
        icon = ':small_red_triangle:'
        if self.status == 'warn':
            icon = ':warning:'

        if slack_workflows:
            return {
                'alert_title': 'dbt test alert',
                'alert_type': self.test_type,
                'table_name': self.table_full_name,
                'detected_at': self.detected_at,
                'owners': self.owners,
                'tags': self.tags,
                'test_name': self.test_name,
                'status': self.status,
                'alert_results_query': self.test_results_query,
                'test_params': self.test_params,
                'error_message': self.error_message,
                'column_name': self.column_name,
                'test_results_query': self.test_results_query,
                'test_rows_sample': self.test_rows_sample,
                'test_runs': self.test_runs,
                'failed_rows_count': self.failed_rows_count
            }
        else:
            slack_message = {"attachments": [{"blocks": []}]}
            self.add_text_section_to_slack_message(slack_message, f"{icon} *dbt test alert*")
            self.add_fields_section_to_slack_message(slack_message,
                                                     [f"*Table*\n{self.table_full_name}",
                                                      f"*When*\n{self.detected_at}"],
                                                     divider=True)
            self.add_fields_section_to_slack_message(slack_message,
                                                     [f"*Status*\n{self.status}", f"*Test name*\n{self.test_name}"])
            self.add_fields_section_to_slack_message(slack_message,
                                                     [f"*Owners*\n{self.owners}", f"*Tags*\n{self.tags}"])
            if self.error_message:
                self.add_text_section_to_slack_message(slack_message,
                                                       f"*Error message*\n{self.error_message}",
                                                       divider=True)
            if self.column_name:
                self.add_text_section_to_slack_message(slack_message, f"*Column*\n{self.column_name}", divider=True)
            if self.test_params:
                self.add_text_section_to_slack_message(slack_message,
                                                       f"*Test Parameters*\n`{self.test_params}`",
                                                       divider=True)
            if self.test_results_query:
                self.add_text_section_to_slack_message(slack_message,
                                                       f"*Test Query*\n```{self.test_results_query}```",
                                                       divider=True)
            if self.test_rows_sample:
                self.add_text_section_to_slack_message(slack_message,
                                                       f"*Test Results Sample*\n`{self.test_rows_sample}`",
                                                       divider=True)
            return slack_message

    def to_test_result_api_dict(self):
        test_runs = {**self.test_runs, 'display_name': self.test_display_name} if self.test_runs else {}

        return {
            'metadata': {
                'test_unique_id': self.test_unique_id,
                'database_name': self.database_name,
                'schema_name': self.schema_name,
                'table_name': self.table_name,
                'column_name': self.column_name,
                'test_name': self.test_name,
                'test_display_name': self.test_display_name,
                'latest_run_time': self.detected_at,
                'latest_run_time_utc': self.detected_at_utc,
                'latest_run_status': self.status,
                'model_unique_id': self.model_unique_id,
                'table_unique_id': self.table_full_name,
                'test_type': self.test_type,
                'test_sub_type': self.test_sub_type,
                'test_query': self.test_results_query,
                'test_params': self.test_params
            },
            'test_results': {
                'display_name': self.test_display_name + ' - failed results sample',
                'results_sample': self.test_rows_sample,
                'error_message': self.error_message,
                'failed_rows_count': self.failed_rows_count
            },
            'test_runs': test_runs
        }


class ElementaryTestResult(DbtTestResult):
    def __init__(
        self,
        id,
        model_unique_id,
        test_unique_id,
        detected_at,
        database_name,
        schema_name,
        table_name,
        column_name,
        test_type,
        test_sub_type,
        test_results_description,
        owners,
        tags,
        test_results_query,
        test_rows_sample,
        other,
        test_name,
        test_params,
        severity,
        status,
        test_runs = None,
        **kwargs
    ) -> None:
        super().__init__(
            id,
            model_unique_id,
            test_unique_id,
            detected_at,
            database_name,
            schema_name,
            table_name,
            column_name,
            test_type,
            test_sub_type,
            test_results_description,
            owners,
            tags,
            test_results_query,
            test_rows_sample,
            other,
            test_name,
            test_params,
            severity,
            status,
            test_runs
        )
        self.test_results_description = self.description_display_name(test_results_description)

    def to_slack_message(self, slack_workflows: bool = False) -> dict:
        anomalous_value = None
        if self.test_type == 'schema_change':
            alert_title = 'Schema change detected'
            sub_type_title = 'Change Type'
        elif self.test_type == 'anomaly_detection':
            alert_title = 'Data anomaly detected'
            sub_type_title = 'Anomaly Type'
            anomalous_value = self.other if self.other else None
        else:
            return {}

        if slack_workflows:
            return {
                'alert_title': alert_title,
                'table_name': self.table_full_name,
                'detected_at': self.detected_at,
                'test_name': self.test_name,
                'alert_type': self.test_type,
                'alert_sub_type': self.test_sub_type_display_name,
                'owners': self.owners,
                'tags': self.tags,
                'description': self.test_results_description,
                'status': self.status,
                'alert_results_query': self.test_results_query,
                'test_params': self.test_params,
                'type': self.test_type,
                'column_name': self.column_name
            }
        else:
            slack_message = {"attachments": [{"blocks": []}]}
            icon = ':small_red_triangle:'
            if self.status == 'warn':
                icon = ':warning:'
            self.add_text_section_to_slack_message(slack_message, f"{icon} *{alert_title}*")
            self.add_fields_section_to_slack_message(slack_message,
                                                     [f"*Table*\n{self.table_full_name}",
                                                      f"*When*\n{self.detected_at}"],
                                                     divider=True)
            self.add_fields_section_to_slack_message(slack_message,
                                                     [f"*Test name*\n{self.test_name}",
                                                      f"*{sub_type_title}:*\n{self.test_sub_type_display_name}"])
            self.add_fields_section_to_slack_message(slack_message,
                                                     [f"*Owners*\n{self.owners}", f"*Tags*\n{self.tags}"])
            if self.test_results_description:
                self.add_text_section_to_slack_message(slack_message,
                                                       f"*Description*\n{self.test_results_description}",
                                                       divider=True)
            column_msgs = []
            if self.column_name:
                column_msgs.append(f"*Column*\n{self.column_name}")
            if anomalous_value:
                column_msgs.append(f"*Anomalous Value*\n{anomalous_value}")
            if column_msgs:
                self.add_fields_section_to_slack_message(slack_message, column_msgs, divider=True)
            if self.test_params:
                self.add_text_section_to_slack_message(slack_message,
                                                       f"*Test Parameters*\n`{self.test_params}`",
                                                       divider=True)
            return slack_message

    def to_test_result_api_dict(self):
        test_params = try_load_json(self.test_params) or {}
        test_results = None
        if self.test_type == 'anomaly_detection':
            timestamp_column = test_params.get('timestamp_column')
            sensitivity = test_params.get('sensitivity')
            test_params = {'timestamp_column': timestamp_column,
                           'anomaly_threshold': sensitivity}
            self.test_rows_sample.sort(key=lambda metric: metric.get('end_time'))
            test_results = {'display_name': self.test_sub_type_display_name,
                            'metrics': self.test_rows_sample,
                            'result_description': self.test_results_description}
        elif self.test_type == 'schema_change':
            test_results = {'display_name': self.test_sub_type_display_name.lower(),
                            'result_description': self.test_results_description}
        test_runs = {**self.test_runs, 'display_name': self.test_sub_type_display_name} if self.test_runs else {}

        return {
            'metadata': {
                'test_unique_id': self.test_unique_id,
                'database_name': self.database_name,
                'schema_name': self.schema_name,
                'table_name': self.table_name,
                'column_name': self.column_name,
                'test_name': self.test_name,
                'test_display_name': self.test_display_name,
                'latest_run_time': self.detected_at,
                'latest_run_time_utc': self.detected_at_utc,
                'latest_run_status': self.status,
                'model_unique_id': self.model_unique_id,
                'table_unique_id': self.table_full_name,
                'test_type': self.test_type,
                'test_sub_type': self.test_sub_type,
                'test_query': self.test_results_query,
                'test_params': test_params
            },
            'test_results': test_results,
            'test_runs': test_runs
        }
