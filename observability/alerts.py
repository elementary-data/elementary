from datetime import datetime
import requests
import json


class Alert(object):
    ALERT_DESCRIPTION = None

    @staticmethod
    def send(webhook: str, data: dict, content_types: str = "application/json"):
        requests.post(url=webhook, headers={'Content-type': content_types}, data=json.dumps(data))

    def to_slack_message(self):
        pass

    def send_to_slack(self, webhook: str):
        data = self.to_slack_message()
        self.send(webhook, data)


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

