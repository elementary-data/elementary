from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, validator

from elementary.monitor.fetchers.invocations.schema import DbtInvocationSchema
from elementary.monitor.fetchers.tests.schema import (
    ModelUniqueIdType,
    TestResultSchema,
    TestRunSchema,
    TotalsSchema,
)
from elementary.utils.log import get_logger
from elementary.utils.time import DATETIME_FORMAT, convert_local_time_to_timezone

logger = get_logger(__name__)


class SelectorFilterSchema(BaseModel):
    selector: Optional[str] = None
    invocation_id: Optional[str] = None
    invocation_time: Optional[str] = None
    last_invocation: Optional[bool] = False
    tag: Optional[str] = None
    owner: Optional[str] = None
    model: Optional[str] = None
    node_names: Optional[List[str]] = None

    @validator("invocation_time", pre=True)
    def format_invocation_time(cls, invocation_time):
        if invocation_time:
            try:
                invocation_datetime = convert_local_time_to_timezone(
                    datetime.fromisoformat(invocation_time)
                )
                return invocation_datetime.strftime(DATETIME_FORMAT)
            except ValueError as err:
                logger.error(
                    f"Failed to parse invocaton time filter: {err}\nPlease use a valid ISO 8601 format"
                )
                raise
        return None


class DataMonitoringReportTestResultsSchema(BaseModel):
    results: Dict[Optional[ModelUniqueIdType], List[TestResultSchema]] = dict()
    totals: Dict[Optional[str], TotalsSchema] = dict()
    invocation: DbtInvocationSchema = dict()


class DataMonitoringReportTestRunsSchema(BaseModel):
    runs: Dict[Optional[ModelUniqueIdType], List[TestRunSchema]] = dict()
    totals: Dict[Optional[str], TotalsSchema] = dict()
