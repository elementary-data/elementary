import requests
import json
from exceptions.exceptions import InvalidAlertType
from utils.time import convert_utc_time_to_local_time
from datetime import datetime


class Alert(object):
    def __init__(self, alert_id) -> None:
        self.alert_id = alert_id

    @staticmethod
    def create_alert_from_row(alert_row: dict) -> 'Alert':
        alert_type = alert_row.get('alert_type')
        if alert_type == 'dbt_test':
            return DbtTestAlert(*alert_row.values())
        else:
            return ElementaryDataAlert(*alert_row.values())

    @staticmethod
    def send(webhook: str, data: dict, content_types: str = "application/json"):
        requests.post(url=webhook, headers={'Content-type': content_types}, data=json.dumps(data))

    def to_slack_message(self) -> dict:
        pass

    def to_slack_workflows_message(self) -> dict:
        pass

    @staticmethod
    def add_fields_section_to_slack_message(slack_message: dict, section_msgs: list, divider: bool = False):
        fields = []
        for section_msg in section_msgs:
            fields.append({
                    "type": "mrkdwn",
                    "text": section_msg
            })

        block = []
        if divider:
            block.append({"type": "divider"})
        block.append({"type": "section", "fields": fields})
        slack_message['attachments'][0]['blocks'].extend(block)

    @staticmethod
    def add_text_section_to_slack_message(slack_message: dict, section_msg: str, divider: bool = False):
        block = []
        if divider:
            block.append({"type": "divider"})
        block.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": section_msg
            }
        })
        slack_message['attachments'][0]['blocks'].extend(block)

    def send_to_slack(self, webhook: str, is_slack_workflow: bool = False):
        if is_slack_workflow:
            data = self.to_slack_workflows_message()
        else:
            data = self.to_slack_message()
        self.send(webhook, data)

    @property
    def id(self) -> str:
        return self.alert_id


class DbtTestAlert(Alert):

    def __init__(self, alert_id, detected_at, database_name, schema_name, table_name, column_name, alert_type, sub_type,
                 alert_description, owners, tags, alert_results_query, alert_results, other, test_name, test_params,
                 severity, status) -> None:
        super().__init__(alert_id)
        self.alert_type = alert_type
        self.alert_title = "dbt test alert"
        self.table_name = '.'.join([database_name, schema_name, table_name]).lower()
        self.detected_at = None
        if detected_at:
            detected_at = datetime.fromisoformat(detected_at)
            self.detected_at = convert_utc_time_to_local_time(detected_at).strftime('%Y-%m-%d %H:%M:%S')
        self.owners = json.loads(owners) if owners else ''
        if isinstance(self.owners, list):
            self.owners = ', '.join(self.owners)
        self.tags = json.loads(tags) if tags else ''
        if isinstance(self.tags, list):
            self.tags = [f'#{tag}' for tag in self.tags]
            self.tags = ', '.join(self.tags)
        self.test_name = test_name
        self.status = status
        self.other = other
        self.sub_type = sub_type
        self.alert_results_query = f'```{alert_results_query.strip()}```' if alert_results_query else ''
        self.alert_results = f'`{alert_results}`' if alert_results else ''
        self.test_params = f"`{test_params}`" if test_params else ''
        self.error_message = alert_description if alert_description else 'No error message'
        self.column_name = column_name if column_name else ''
        self.icon = ':small_red_triangle:'
        if severity and severity.lower() == 'warn':
            self.icon = ':warning:'

    def to_slack_message(self) -> dict:
        slack_message = {
            "attachments": [
                {
                    "blocks": [
                    ]
                }
            ]
        }

        self.add_text_section_to_slack_message(slack_message, f"{self.icon} *{self.alert_title}*")

        self.add_fields_section_to_slack_message(slack_message,
                                                 [f"*Table:*\n{self.table_name}", f"*When:*\n{self.detected_at}"],
                                                 divider=True)

        self.add_fields_section_to_slack_message(slack_message,
                                                 [f"*Status:*\n{self.status}", f"*Test name:*\n{self.test_name}"])

        self.add_fields_section_to_slack_message(slack_message, [f"*Owners:*\n{self.owners}", f"*Tags:*\n{self.tags}"])

        if self.error_message:
            self.add_text_section_to_slack_message(slack_message,
                                                   f"*Error message:*\n{self.error_message}",
                                                   divider=True)

        if self.column_name:
            self.add_text_section_to_slack_message(slack_message, f"*Column:*\n{self.column_name}", divider=True)

        if self.test_params:
            self.add_text_section_to_slack_message(slack_message,
                                                   f"*Test Parameters:*\n{self.test_params}",
                                                   divider=True)

        if self.alert_results_query:
            self.add_text_section_to_slack_message(slack_message,
                                                   f"*Test Query:*\n{self.alert_results_query}",
                                                   divider=True)

        if self.alert_results:
            self.add_text_section_to_slack_message(slack_message,
                                                   f"*Test Results Sample:*\n{self.alert_results}",
                                                   divider=True)

        return slack_message

    def to_slack_workflows_message(self) -> dict:
        return {
            'alert_title': self.alert_title,
            'alert_type': self.alert_type,
            'table_name': self.table_name,
            'detected_at': self.detected_at,
            'owners': self.owners,
            'tags': self.tags,
            'test_name': self.test_name,
            'status': self.status,
            'alert_results_query': self.alert_results_query,
            'alert_results': self.alert_results,
            'test_params': self.test_params,
            'error_message': self.error_message,
            'column_name': self.column_name
        }


class ElementaryDataAlert(DbtTestAlert):
    def __init__(self, alert_id, detected_at, database_name, schema_name, table_name, column_name,
                 alert_type, sub_type, alert_description, owners, tags, alert_results_query, alert_results, other,
                 test_name, test_params, severity, status) -> None:
        super().__init__(alert_id, detected_at, database_name, schema_name, table_name, column_name,
                         alert_type, sub_type, alert_description, owners, tags, alert_results_query, alert_results,
                         other, test_name, test_params, severity, status)

        self.anomalous_value = None
        if self.alert_type == 'schema_change':
            self.alert_title = 'Schema change detected'
            self.sub_type_title = 'Change Type'
        elif self.alert_type == 'anomaly_detection':
            self.alert_title = 'Data anomaly detected'
            self.sub_type_title = 'Anomaly Type'
            self.anomalous_value = self.other if self.other else None
        else:
            raise InvalidAlertType(f'Got invalid alert type - {self.alert_type}')

        self.sub_type_value = ' '.join([word[0].upper() + word[1:] for word in self.sub_type.split('_')])
        self.description = alert_description[0].upper() + alert_description[1:].lower() if alert_description else ''

    def to_slack_message(self) -> dict:
        slack_message = {
            "attachments": [
                {
                    "blocks": [
                    ]
                }
            ]
        }

        self.add_text_section_to_slack_message(slack_message, f"{self.icon} *{self.alert_title}*")

        self.add_fields_section_to_slack_message(slack_message,
                                                 [f"*Table:*\n{self.table_name}", f"*When:*\n{self.detected_at}"],
                                                 divider=True)

        self.add_fields_section_to_slack_message(slack_message,
                                                 [f"*Test name:*\n{self.test_name}",
                                                  f"*{self.sub_type_title}:*\n{self.sub_type_value}"])

        self.add_fields_section_to_slack_message(slack_message, [f"*Owners:*\n{self.owners}", f"*Tags:*\n{self.tags}"])

        if self.description:
            self.add_text_section_to_slack_message(slack_message, f"*Description:*\n{self.description}", divider=True)

        column_msgs = []
        if self.column_name:
            column_msgs.append(f"*Column:*\n{self.column_name}")
        if self.anomalous_value:
            column_msgs.append(f"*Anomalous Value:*\n{self.anomalous_value}")

        if column_msgs:
            self.add_fields_section_to_slack_message(slack_message, column_msgs, divider=True)

        if self.test_params:
            self.add_text_section_to_slack_message(slack_message,
                                                   f"*Test Parameters:*\n{self.test_params}",
                                                   divider=True)

        return slack_message

    def to_slack_workflows_message(self) -> dict:
        return {
            'alert_description': self.alert_title,  # backwards
            'alert_title': self.alert_title,
            'alert_type': self.alert_type,
            'table_name': self.table_name,
            'detected_at': self.detected_at,
            'owners': self.owners,
            'tags': self.tags,
            'test_name': self.test_name,
            'status': self.status,
            'alert_results_query': self.alert_results_query,
            'alert_results': self.alert_results,
            'test_params': self.test_params,
            'description': self.description,  # backwards
            'sub_type': self.sub_type_value,
            'type': self.sub_type_value,   # backwards
            'column_name': self.column_name
        }




