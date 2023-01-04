from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, validator

from elementary.utils.log import get_logger
from elementary.utils.time import DATETIME_FORMAT, convert_local_time_to_timezone

logger = get_logger(__name__)


class ReportFilter(BaseModel):
    invocation_id: Optional[str] = None
    invocation_time: Optional[str] = None
    last_invocation: Optional[bool] = False

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


class AlertsFilter(BaseModel):
    tag: Optional[str] = None
    owner: Optional[str] = None
    model: Optional[str] = None
    node_names: Optional[List[str]] = None
