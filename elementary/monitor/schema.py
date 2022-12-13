from typing import Optional

from pydantic import BaseModel, validator

from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class DataMonitoringFilter(BaseModel):
    invocation_id: Optional[str] = None
    invocation_time: Optional[str] = None
    last_invocation: Optional[bool] = False

    @validator("invocation_time", pre=True)
    def format_invocation_time(cls, invocation_time):
        if invocation_time:
            return convert_partial_iso_format_to_full_iso_format(invocation_time)
        return None
