from typing import Optional

from elementary.utils.json_utils import try_load_json
from elementary.utils.pydantic_shim import BaseModel, validator
from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class DbtInvocationSchema(BaseModel):
    invocation_id: Optional[str] = None
    detected_at: Optional[str] = None
    command: Optional[str] = None
    selected: Optional[str] = None
    full_refresh: Optional[bool] = None
    job_url: Optional[str] = None
    job_name: Optional[str] = None
    job_id: Optional[str] = None
    orchestrator: Optional[str] = None

    @validator("detected_at", pre=True)
    def format_detected_at(cls, detected_at):
        return convert_partial_iso_format_to_full_iso_format(detected_at)

    @validator("selected", pre=True)
    def format_selected(cls, selected):
        selected_list = try_load_json(selected) or []
        return " ".join(selected_list)
