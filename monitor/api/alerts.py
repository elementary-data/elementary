import json
from typing import Callable, List

import utils.dbt
from clients.api.api import APIClient
from monitor.alerts.alerts import AlertsQueryResult, Alerts
from monitor.alerts.malformed import MalformedAlert
from monitor.alerts.model import ModelAlert
from monitor.alerts.test import TestAlert
from utils.log import get_logger

logger = get_logger(__name__)


class AlertsAPI(APIClient):
    def query(self, days_back: int) -> Alerts:
        return Alerts(
            tests=self._query_test_alerts(days_back),
            models=self._query_model_alerts(days_back),
        )

    def _query_test_alerts(self, days_back: int) -> AlertsQueryResult[TestAlert]:
        logger.info('Querying test alerts.')
        return self._query_alert_type(
            {'macro_name': 'get_new_test_alerts', 'macro_args': {'days_back': days_back}},
            TestAlert.create_test_alert_from_dict
        )

    def _query_model_alerts(self, days_back: int) -> AlertsQueryResult[ModelAlert]:
        logger.info('Querying model alerts.')
        return self._query_alert_type(
            {'macro_name': 'get_new_model_alerts', 'macro_args': {'days_back': days_back}},
            ModelAlert
        )

    def _query_alert_type(self, run_operation_args: dict, alert_factory_func: Callable) -> AlertsQueryResult:
        raw_alerts = self.dbt_runner.run_operation(**run_operation_args)
        alerts = []
        malformed_alerts = []
        if raw_alerts:
            alert_dicts = json.loads(raw_alerts[0])
            for alert_dict in alert_dicts:
                try:
                    alerts.append(alert_factory_func(
                        elementary_database_and_schema=self.elementary_database_and_schema,
                        **alert_dict
                    ))
                except Exception:
                    malformed_alerts.append(MalformedAlert(
                        alert_dict['id'],
                        self.elementary_database_and_schema,
                        alert_dict,
                    ))
        if malformed_alerts:
            logger.error('Failed to parse some alerts.')
            self.success = False
        return AlertsQueryResult(alerts, malformed_alerts)

    def update_sent_alerts(self, alert_ids: List[str], table_name: str) -> None:
        alert_ids_chunks = self._split_list_to_chunks(alert_ids)
        for alert_ids_chunk in alert_ids_chunks:
            self.dbt_runner.run_operation(
                macro_name='update_sent_alerts',
                macro_args={'alert_ids': alert_ids_chunk, 'table_name': table_name},
                json_logs=False
            )

    @property
    def elementary_database_and_schema(self):
        try:
            return utils.dbt.get_elementary_database_and_schema(self.dbt_runner)
        except Exception:
            logger.error("Failed to parse Elementary's database and schema.")
            return '<elementary_database>.<elementary_schema>'

    @staticmethod
    def _split_list_to_chunks(items: list, chunk_size: int = 50) -> List[List]:
        chunk_list = []
        for i in range(0, len(items), chunk_size):
            chunk_list.append(items[i: i + chunk_size])
        return chunk_list
