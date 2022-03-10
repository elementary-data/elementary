import requests
import json
from exceptions.exceptions import InvalidAlertType
from utils.time import convert_utc_time_to_local_time


class Alert(object):
    ALERT_DESCRIPTION = None

    def __init__(self, alert_id) -> None:
        self.alert_id = alert_id

    @staticmethod
    def create_alert_from_row(alert_row: list) -> 'Alert':
        alert_id, detected_at, database_name, schema_name, table_name, column_name, alert_type, sub_type, \
            alert_description = alert_row
        if alert_type == 'schema_change':
            return SchemaChangeAlert(alert_id, database_name, schema_name, table_name, detected_at, sub_type,
                                     alert_description)
        elif alert_type == 'anomaly_detection':
            return AnomalyDetectionAlert(alert_id, database_name, schema_name, table_name, detected_at, sub_type,
                                         alert_description)
        else:
            raise InvalidAlertType(f'Got invalid alert type - {alert_type}')

    @staticmethod
    def send(webhook: str, data: dict, content_types: str = "application/json"):
        requests.post(url=webhook, headers={'Content-type': content_types}, data=json.dumps(data))

    def to_slack_message(self) -> dict:
        pass

    def to_slack_workflows_message(self) -> dict:
        pass

    def send_to_slack(self, webhook: str, is_slack_workflow: bool = False):
        if is_slack_workflow:
            data = self.to_slack_workflows_message()
        else:
            data = self.to_slack_message()
        self.send(webhook, data)

    @property
    def id(self) -> str:
        return self.alert_id


class SchemaChangeAlert(Alert):
    ALERT_DESCRIPTION = "Schema change detected"

    def __init__(self, alert_id, database_name, schema_name, table_name, detected_at, sub_type, description) -> None:
        super().__init__(alert_id)
        self.table_name = '.'.join([database_name, schema_name, table_name]).lower()
        self.detected_at = convert_utc_time_to_local_time(detected_at).strftime('%Y-%m-%d %H:%M:%S')
        self.description = description[0].upper() + description[1:].lower()
        self.change_type = ' '.join([word[0].upper() + word[1:] for word in sub_type.split('_')])

    def to_slack_message(self) -> dict:
        return {
            "blocks": [
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f":small_red_triangle:*{self.ALERT_DESCRIPTION}*"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f">*Table:*\n>{self.table_name}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*When:*\n{self.detected_at}"
                        }
                    ]
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f">*Change Type:*\n>{self.change_type}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Description:*\n{self.description}"
                        }
                    ]
                }
            ]
        }

    def to_slack_workflows_message(self) -> dict:
        return {
            "alert_description": self.ALERT_DESCRIPTION,
            "table_name": self.table_name,
            "detected_at": self.detected_at,
            "type": self.change_type,
            "description": self.description
        }


class AnomalyDetectionAlert(Alert):
    ALERT_DESCRIPTION = "Data anomaly detected"

    def __init__(self, alert_id, database_name, schema_name, table_name, detected_at, sub_type, description) -> None:
        super().__init__(alert_id)
        self.table_name = '.'.join([database_name, schema_name, table_name]).lower()
        self.detected_at = convert_utc_time_to_local_time(detected_at).strftime('%Y-%m-%d %H:%M:%S')
        self.description = description[0].upper() + description[1:].lower()
        self.anomaly_type = ' '.join([word[0].upper() + word[1:] for word in sub_type.split('_')])

    def to_slack_message(self) -> dict:
        return {
            "blocks": [
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f":small_red_triangle:*{self.ALERT_DESCRIPTION}*"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f">*Table:*\n>{self.table_name}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*When:*\n{self.detected_at}"
                        }
                    ]
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f">*Change Type:*\n>{self.anomaly_type}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Description:*\n{self.description}"
                        }
                    ]
                }
            ]
        }

    def to_slack_workflows_message(self) -> dict:
        return {
            "alert_description": self.ALERT_DESCRIPTION,
            "table_name": self.table_name,
            "detected_at": self.detected_at,
            "type": self.anomaly_type,
            "description": self.description
        }
