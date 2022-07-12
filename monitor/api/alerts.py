import functools
import json
from dataclasses import dataclass
from typing import Callable, Optional

from clients.api.api import APIClient
from monitor.alert import AlertsQueryResult, ModelAlert, Alerts, TestAlert, MalformedAlert, DbtTestAlert, \
    ElementaryTestAlert
from utils.log import get_logger

logger = get_logger(__name__)


@dataclass
class AlertsAPI(APIClient):
    def query(self, days_back: int) -> Alerts:
        alerts = Alerts(
            tests=self._query_test_alerts(days_back),
            models=self._query_model_alerts(days_back),
        )
        return alerts

    def _query_test_alerts(self, days_back: int) -> AlertsQueryResult[TestAlert]:
        logger.info('Querying test alerts.')
        return self._query_alert_type(
            {'macro_name': 'get_new_test_alerts', 'macro_args': {'days_back': days_back}},
            self.create_test_alert_from_dict
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

    @property
    @functools.lru_cache
    def elementary_database_and_schema(self):
        try:
            database_and_schema = self.dbt_runner.run_operation('get_elementary_database_and_schema')[0]
            return '.'.join(json.loads(database_and_schema.replace("'", '"')))
        except Exception:
            logger.error("Failed to parse Elementary's database and schema.")
            return '<elementary_database>.<elementary_schema>'

    @staticmethod
    def create_test_alert_from_dict(**test_alert_dict) -> Optional[TestAlert]:
        if test_alert_dict.get('test_type') == 'dbt_test':
            return DbtTestAlert(**test_alert_dict)
        return ElementaryTestAlert(**test_alert_dict)
