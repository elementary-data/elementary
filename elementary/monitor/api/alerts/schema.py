from typing import List, Union

from pydantic import BaseModel

from elementary.monitor.fetchers.alerts.schema.pending_alerts import (
    PendingModelAlertSchema,
    PendingSourceFreshnessAlertSchema,
    PendingTestAlertSchema,
)


class TestAlertsSchema(BaseModel):
    send: List[PendingTestAlertSchema]
    skip: List[PendingTestAlertSchema]


class ModelAlertsSchema(BaseModel):
    send: List[PendingModelAlertSchema]
    skip: List[PendingModelAlertSchema]


class SourceFreshnessAlertsSchema(BaseModel):
    send: List[PendingSourceFreshnessAlertSchema]
    skip: List[PendingSourceFreshnessAlertSchema]


class SortedAlertsSchema(BaseModel):
    send: Union[
        List[PendingTestAlertSchema],
        List[PendingModelAlertSchema],
        List[PendingSourceFreshnessAlertSchema],
    ]
    skip: Union[
        List[PendingTestAlertSchema],
        List[PendingModelAlertSchema],
        List[PendingSourceFreshnessAlertSchema],
    ]

    class Config:
        smart_union = True


class AlertsSchema(BaseModel):
    tests: TestAlertsSchema
    models: ModelAlertsSchema
    source_freshnesses: SourceFreshnessAlertsSchema

    @property
    def all_alerts(
        self,
    ) -> List[
        Union[
            PendingTestAlertSchema,
            PendingModelAlertSchema,
            PendingSourceFreshnessAlertSchema,
        ]
    ]:
        return [*self.tests.send, *self.models.send, *self.source_freshnesses.send]

    @property
    def count(self) -> int:
        return (
            len(self.tests.send)
            + len(self.models.send)
            + len(self.source_freshnesses.send)
        )
