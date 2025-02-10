from datetime import datetime
from typing import Dict, List, Optional

from elementary.monitor.alerts.alert import AlertModel
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    ReportLinkData,
    get_test_runs_link,
)
from elementary.utils.time import (
    convert_datetime_utc_str_to_timezone_str,
    datetime_strftime,
    get_formatted_timedelta,
)


class SourceFreshnessAlertModel(AlertModel):
    def __init__(
        self,
        id: str,
        source_name: str,
        identifier: str,
        original_status: str,
        path: str,
        error: Optional[str],
        alert_class_id: str,
        source_freshness_execution_id: str,
        model_unique_id: Optional[str] = None,
        error_after: Optional[str] = None,
        warn_after: Optional[str] = None,
        snapshotted_at: Optional[datetime] = None,
        max_loaded_at: Optional[datetime] = None,
        max_loaded_at_time_ago_in_s: Optional[float] = None,
        filter: Optional[str] = None,
        freshness_description: Optional[str] = None,
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
        self.snapshotted_at_str = (
            convert_datetime_utc_str_to_timezone_str(
                snapshotted_at.isoformat(), self.timezone
            )
            if snapshotted_at
            else None
        )

        self.max_loaded_at = (
            convert_datetime_utc_str_to_timezone_str(
                max_loaded_at.isoformat(), self.timezone
            )
            if max_loaded_at
            else None
        )

        self.max_loaded_at_time_ago_in_s = max_loaded_at_time_ago_in_s

        formatted_max_loaded_at = (
            convert_datetime_utc_str_to_timezone_str(
                max_loaded_at.isoformat(), self.timezone, include_timezone=True
            )
            if max_loaded_at
            else None
        )
        formatted_detected_at = (
            datetime_strftime(self.detected_at, include_timezone=True)
            if self.detected_at
            else "N/A"
        )
        self.result_description = (
            error
            if error
            else f"When the test ran at {formatted_detected_at}, the most recent record found in the table was {get_formatted_timedelta(self.max_loaded_at_time_ago_in_s or 0)} earlier ({formatted_max_loaded_at})."
        )

        self.source_name = source_name
        self.identifier = identifier
        self.original_status = original_status
        self.error_after = error_after
        self.warn_after = warn_after
        self.filter = filter
        self.path = path
        self.error = error
        self.freshness_description = freshness_description
        self.source_freshness_execution_id = source_freshness_execution_id

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
            source_name=self.source_name,
            identifier=self.identifier,
            original_status=self.original_status,
            error_after=self.error_after,
            warn_after=self.warn_after,
            path=self.path,
            error=self.error,
            snapshotted_at=self.snapshotted_at_str,
            max_loaded_at=self.max_loaded_at,
            max_loaded_at_time_ago_in_s=self.max_loaded_at_time_ago_in_s,
            filter=self.filter,
            freshness_description=self.freshness_description,
        )

    @property
    def concise_name(self) -> str:
        return f"source freshness alert - {self.source_name}.{self.identifier}"

    @property
    def error_message(self) -> str:
        if self.original_status == "runtime error":
            return f"Failed to calculate the source freshness\n```{self.error}```"
        return self.result_description

    @property
    def summary(self) -> str:
        if self.original_status == "runtime error":
            return f'Failed to calculate the source freshness of "{self.source_name}"'
        return f'Freshness exceeded the acceptable times on source "{self.source_name}"'

    def get_report_link(self) -> Optional[ReportLinkData]:
        return get_test_runs_link(self.report_url, self.source_freshness_execution_id)
