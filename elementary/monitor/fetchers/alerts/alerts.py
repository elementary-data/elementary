import json
from typing import Dict, List, Union

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.fetcher.fetcher import FetcherClient
from elementary.config.config import Config
from elementary.monitor.data_monitoring.schema import ResourceType
from elementary.monitor.fetchers.alerts.schema.pending_alerts import (
    PendingModelAlertSchema,
    PendingSourceFreshnessAlertSchema,
    PendingTestAlertSchema,
)
from elementary.utils.log import get_logger
from elementary.utils.time import get_now_utc_str

logger = get_logger(__name__)


class AlertsFetcher(FetcherClient):
    def __init__(
        self,
        dbt_runner: BaseDbtRunner,
        config: Config,
        elementary_database_and_schema: str,
    ):
        super().__init__(dbt_runner)
        self.config = config
        self.elementary_database_and_schema = elementary_database_and_schema

    def skip_alerts(
        self,
        alerts_to_skip: Union[
            List[PendingTestAlertSchema],
            List[PendingModelAlertSchema],
            List[PendingSourceFreshnessAlertSchema],
        ],
        resource_type: ResourceType,
    ):
        table_name = self._resource_type_to_table(resource_type)
        alert_ids = [alert.id for alert in alerts_to_skip]
        alert_ids_chunks = self._split_list_to_chunks(alert_ids)
        logger.info(f'Update skipped alerts at "{table_name}"')
        for alert_ids_chunk in alert_ids_chunks:
            self.dbt_runner.run(
                select="elementary_cli.update_alerts.update_skipped_alerts",
                vars={
                    "alert_ids": alert_ids_chunk,
                    "table_name": table_name,
                },
                quiet=True,
            )

    def query_pending_test_alerts(
        self, days_back: int, disable_samples: bool = False
    ) -> List[PendingTestAlertSchema]:
        logger.info("Querying test alerts.")
        pending_test_alerts_results = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_pending_test_alerts",
            macro_args={
                "days_back": days_back,
                "disable_samples": disable_samples,
            },
        )
        return [
            PendingTestAlertSchema(**result)
            for result in json.loads(pending_test_alerts_results[0])
        ]

    def query_pending_model_alerts(
        self, days_back: int
    ) -> List[PendingModelAlertSchema]:
        logger.info("Querying model alerts.")
        pending_model_alerts_results = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_pending_model_alerts",
            macro_args={"days_back": days_back},
        )
        return [
            PendingModelAlertSchema(**result)
            for result in json.loads(pending_model_alerts_results[0])
        ]

    def query_pending_source_freshness_alerts(
        self, days_back: int
    ) -> List[PendingSourceFreshnessAlertSchema]:
        logger.info("Querying source freshness alerts.")
        pending_source_freshness_alerts_results = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_pending_source_freshness_alerts",
            macro_args={"days_back": days_back},
        )
        return [
            PendingSourceFreshnessAlertSchema(**result)
            for result in json.loads(pending_source_freshness_alerts_results[0])
        ]

    def query_last_test_alert_times(self, days_back: int) -> Dict[str, str]:
        logger.info("Querying test alerts last sent times.")
        response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_last_test_alert_sent_times",
            macro_args={"days_back": days_back},
        )
        return json.loads(response[0])

    def query_last_model_alert_times(self, days_back: int) -> Dict[str, str]:
        logger.info("Querying model alerts last sent times.")
        response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_last_model_alert_sent_times",
            macro_args={"days_back": days_back},
        )
        return json.loads(response[0])

    def query_last_source_freshness_alert_times(self, days_back: int) -> Dict[str, str]:
        logger.info("Querying source freshness alerts last sent times.")
        response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_last_source_freshness_alert_sent_times",
            macro_args={"days_back": days_back},
        )
        return json.loads(response[0])

    def update_sent_alerts(
        self, alert_ids: List[str], resource_type: ResourceType
    ) -> None:
        table_name = self._resource_type_to_table(resource_type)
        alert_ids_chunks = self._split_list_to_chunks(alert_ids)
        logger.info(f'Update sent alerts at "{table_name}"')
        for alert_ids_chunk in alert_ids_chunks:
            self.dbt_runner.run(
                select="elementary_cli.update_alerts.update_sent_alerts",
                vars={
                    "alert_ids": alert_ids_chunk,
                    "sent_at": get_now_utc_str(),
                    "table_name": table_name,
                },
                quiet=True,
            )

    @staticmethod
    def _split_list_to_chunks(items: list, chunk_size: int = 50) -> List[List]:
        chunk_list = []
        for i in range(0, len(items), chunk_size):
            chunk_list.append(items[i : i + chunk_size])
        return chunk_list

    @staticmethod
    def _resource_type_to_table(resource_type: ResourceType) -> str:
        if resource_type == ResourceType.TEST:
            return "alerts"
        elif resource_type == ResourceType.MODEL:
            return "alerts_models"
        else:
            return "alerts_source_freshness"
