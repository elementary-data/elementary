from typing import List

from elementary.monitor.fetchers.alerts.schema.pending_alerts import PendingAlertSchema
from elementary.utils.pydantic_shim import BaseModel


class SortedAlertsSchema(BaseModel):
    send: List[PendingAlertSchema]
    skip: List[PendingAlertSchema]
