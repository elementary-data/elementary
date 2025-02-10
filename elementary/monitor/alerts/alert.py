from datetime import datetime
from typing import Dict, List, Optional

from dateutil import tz

from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    ReportLinkData,
)
from elementary.utils.log import get_logger
from elementary.utils.time import DATETIME_WITH_TIMEZONE_FORMAT

logger = get_logger(__name__)


class AlertModel:
    def __init__(
        self,
        id: str,
        alert_class_id: str,
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
        self.id = id
        self.alert_class_id = alert_class_id
        self.detected_at_utc = None
        self.detected_at = None
        if detected_at is not None:
            try:
                self.detected_at_utc = detected_at
                self.detected_at = detected_at.astimezone(
                    tz.gettz(timezone) if timezone else tz.tzlocal()
                )
            except Exception:
                logger.error('Failed to parse "detected_at" field.')
        self.detected_at_str = (
            self.detected_at.strftime(DATETIME_WITH_TIMEZONE_FORMAT).strip()
            if self.detected_at
            else "N/A"
        )
        self.database_name = database_name
        self.schema_name = schema_name
        self.owners: List[str] = owners or []
        self.tags: List[str] = tags or []
        self.subscribers: List[str] = subscribers or []
        self.model_meta = model_meta or dict()
        self.status = status
        self.model_unique_id = model_unique_id
        self.suppression_interval = suppression_interval
        self.timezone = timezone
        self.report_url = report_url
        self.alert_fields = alert_fields
        self.elementary_database_and_schema = elementary_database_and_schema
        self.env = env

    @property
    def unified_meta(self) -> Dict:
        return self.model_meta

    @property
    def data(self) -> Dict:
        raise NotImplementedError

    @property
    def concise_name(self):
        return "Alert"

    @property
    def summary(self) -> str:
        raise NotImplementedError

    def get_report_link(self) -> Optional[ReportLinkData]:
        raise NotImplementedError
