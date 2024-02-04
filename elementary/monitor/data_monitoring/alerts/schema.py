from typing import List

from pydantic import BaseModel

from elementary.monitor.fetchers.alerts.schema.pending_alerts import PendingAlertSchema


class SortedAlertsSchema(BaseModel):
    send: List[PendingAlertSchema]
    skip: List[PendingAlertSchema]
