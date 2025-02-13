from datetime import datetime
from typing import Dict, List, Optional

from elementary.monitor.alerts.alert import AlertModel
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    ReportLinkData,
    get_model_runs_link,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class ModelAlertModel(AlertModel):
    def __init__(
        self,
        id: str,
        alias: str,
        path: str,
        original_path: str,
        materialization: str,
        full_refresh: bool,
        alert_class_id: str,
        message: Optional[str] = None,
        model_unique_id: Optional[str] = None,
        detected_at: Optional[datetime] = None,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        owners: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        subscribers: Optional[List[str]] = None,
        status: Optional[str] = None,
        model_meta: Optional[Dict] = None,
        suppression_interval: Optional[int] = None,
        timezone: Optional[str] = None,
        report_url: Optional[str] = None,
        alert_fields: Optional[List[str]] = None,
        elementary_database_and_schema: Optional[str] = None,
        env: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(
            id,
            alert_class_id,
            model_unique_id,
            detected_at,
            database_name,
            schema_name,
            owners,
            tags,
            subscribers,
            status,
            model_meta,
            suppression_interval,
            timezone,
            report_url,
            alert_fields,
            elementary_database_and_schema,
            env=env,
        )
        self.alias = alias
        self.path = path
        self.original_path = original_path
        self.materialization = materialization
        self.message = message
        self.full_refresh = full_refresh

    @property
    def data(self) -> Dict:
        return dict(
            id=self.id,
            alert_class_id=self.alert_class_id,
            model_unique_id=self.model_unique_id,
            detected_at=self.detected_at_str,
            database_name=self.database_name,
            schema_name=self.schema_name,
            owners=self.owners,
            tags=self.tags,
            subscribers=self.subscribers,
            status=self.status,
            suppression_interval=self.suppression_interval,
            alias=self.alias,
            path=self.path,
            original_path=self.original_path,
            materialization=self.materialization,
            message=self.message,
            full_refresh=self.full_refresh,
        )

    @property
    def concise_name(self) -> str:
        if self.materialization == "snapshot":
            dbt_type = "snapshot"
        else:
            dbt_type = "model"
        return f"dbt {dbt_type} alert - {self.alias}"

    @property
    def summary(self) -> str:
        return f'dbt failed to build {self.materialization} "{self.alias}"'

    def get_report_link(self) -> Optional[ReportLinkData]:
        return get_model_runs_link(self.report_url, self.model_unique_id)
