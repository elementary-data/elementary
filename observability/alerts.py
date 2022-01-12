from datetime import datetime
import requests
import json

from exceptions.exceptions import InvalidAlertType


class Alert(object):
    ALERT_DESCRIPTION = None

    @staticmethod
    def create_alert_from_row(alert_row: list) -> 'Alert':
        alert_type = alert_row[0]
        #TODO: change alert type to be similar to alert description
        if alert_type in {'table_schema_change', 'column_schema_change'}:
            return SchemaChangeAlert(alert_row[1],
                                     alert_row[2],
                                     alert_row[3],
                                     alert_row[4],
                                     alert_row[5])
        else:
            raise InvalidAlertType(f'Got invalid alert type - {alert_type}')

    @staticmethod
    def send(webhook: str, data: dict, content_types: str = "application/json"):
        requests.post(url=webhook, headers={'Content-type': content_types}, data=json.dumps(data))

    def to_slack_message(self):
        pass

    def send_to_slack(self, webhook: str):
        data = self.to_slack_message()
        self.send(webhook, data)


class SchemaChangeAlert(Alert):
    ALERT_DESCRIPTION = "Schema change detected"

    def __init__(self, table_name, detected_at, description, alert_details_keys, alert_details_values):
        self.table_name = table_name
        self.detected_at = detected_at
        self.description = description
        self.alert_details_keys = alert_details_keys
        self.alert_details_values = alert_details_values

    def to_slack_message(self) -> dict:
        return {
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"You have a new alert:\n*{self.ALERT_DESCRIPTION}*"
                    }
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
                            "text": f"*Description:*\n{self.description}"
                        }
                    ]
                }
            ]
        }


class SchemaChangeUnstructuredDataAlert(Alert):
    ALERT_DESCRIPTION = "Schema change in unstructured data detected"

    def __init__(self, table_name, total_rows, bad_rows, bad_rows_rate, batch_min_time,
                 batch_max_time, validation_details):
        self.table_name = table_name
        self.total_rows = total_rows
        self.bad_rows = bad_rows
        self.bad_rows_rate = bad_rows_rate
        self.batch_min_time = batch_min_time
        self.batch_max_time = batch_max_time
        self.validation_details = validation_details

    def to_slack_message(self) -> dict:
        return {
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"You have a new alert:\n*{self.ALERT_DESCRIPTION}*"
                    }
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
                            "text": f"*When:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                        }
                    ]
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Batch:*\n{self.batch_min_time.strftime('%Y-%m-%d %H:%M:%S')} - "
                                    f"{self.batch_max_time.strftime('%Y-%m-%d %H:%M:%S')}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Description:*\nBad rows rate is {self.bad_rows_rate}%!\n"
                                    f"Out of {self.total_rows} rows, {self.bad_rows} rows did not fit "
                                    f"any of your JSON schemas."
                        }
                    ]
                }
            ]
        }

