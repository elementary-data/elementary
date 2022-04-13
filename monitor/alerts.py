import requests
import json
from exceptions.exceptions import InvalidAlertType
from utils.time import convert_utc_time_to_local_time
from datetime import datetime


class Alert(object):
    ALERT_DESCRIPTION = None

    def __init__(self, alert_id) -> None:
        self.alert_id = alert_id

    @staticmethod
    def create_alert_from_row(alert_row: dict) -> 'Alert':
        alert_id, detected_at, database_name, schema_name, table_name, column_name, alert_type, sub_type, \
            alert_description, owner, tags, alert_results_query, other = alert_row.values()
        detected_at = datetime.fromisoformat(detected_at)
        if alert_type == 'schema_change':
            return SchemaChangeAlert(alert_id, database_name, schema_name, table_name, detected_at, sub_type,
                                     alert_description)
        elif alert_type == 'anomaly_detection':
            return AnomalyDetectionAlert(alert_id, database_name, schema_name, table_name, detected_at, sub_type,
                                         alert_description)
        elif alert_type == 'dbt_test':
            return DbtTestAlert(alert_id, database_name, schema_name, table_name, detected_at, sub_type,
                                alert_description, owner, tags, alert_results_query, other)
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
        self.description = description[0].upper() + description[1:].lower() if description else ''
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
        self.description = description if description else ''
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
                            "text": f">*Anomaly Type:*\n>{self.anomaly_type}"
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


class DbtTestAlert(Alert):
    ALERT_DESCRIPTION = "dbt test alert"

    def __init__(self, alert_id, database_name, schema_name, table_name, detected_at, sub_type, description,
                 owner, tags, alert_results_query, other) -> None:
        super().__init__(alert_id)
        self.table_name = '.'.join([database_name, schema_name, table_name]).lower()
        self.detected_at = convert_utc_time_to_local_time(detected_at).strftime('%Y-%m-%d %H:%M:%S')
        self.owners = json.loads(owner) if owner else ''
        if isinstance(self.owners, list):
            self.owners = ', '.join(self.owners)
        self.tags = json.loads(tags) if tags else ''
        if isinstance(self.tags, list):
            self.tags = [f'#{tag}' for tag in self.tags]
            self.tags = ', '.join(self.tags)
        self.status = sub_type
        self.dbt_test_query = f'```{alert_results_query.strip()}```' if alert_results_query else ''
        test_metadata = json.loads(other) if other else {}
        self.test_name = test_metadata.get('name')
        self.test_kwargs = f"`{test_metadata.get('kwargs')}`"
        self.error_message = description if description else 'No error message'

    def to_slack_message(self) -> dict:
        return {
            "attachments": [
                {
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f":small_red_triangle:*{self.ALERT_DESCRIPTION}*"
                            }
                        },
                        {
                            "type": "divider"
                        },
                        {
                            "type": "section",
                            "fields": [
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Table:*\n{self.table_name}"
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
                                    "text": f"*Status:*\n{self.status}"
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Test name:*\n{self.test_name}"
                                }
                            ]
                        },
                        {
                            "type": "section",
                            "fields": [
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Owners:*\n{self.owners}"
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Tags:*\n{self.tags}"
                                }
                            ]
                        },
                        {
                            "type": "divider"
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Error message:*\n{self.error_message}"
                            }
                        },
                        {
                            "type": "divider"
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Test Parameters:*\n{self.test_kwargs}"
                            }
                        },
                        {
                            "type": "divider"
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Test Query:*\n{self.dbt_test_query}"
                            }
                        }
                    ]
                }
            ]
        }

    def to_slack_workflows_message(self) -> dict:
        #TODO: implement this func
        pass