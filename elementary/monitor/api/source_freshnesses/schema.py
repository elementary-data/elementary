from typing import List, Optional

from elementary.monitor.api.totals_schema import TotalsSchema
from elementary.utils.pydantic_shim import BaseModel, Field, validator
from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class DbtSourceFreshnessResultSchema(BaseModel):
    status: str
    error_message: Optional[str] = None
    max_loaded_at_time_ago_in_s: Optional[float] = None
    max_loaded_at: Optional[str] = None
    detected_at: Optional[str] = None


class SourceFreshnessMetadataSchema(BaseModel):
    test_unique_id: str
    database_name: Optional[str] = None
    schema_name: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    test_name: str
    latest_run_time: str
    latest_run_status: str
    model_unique_id: Optional[str] = None
    test_type: str
    test_sub_type: str
    description: Optional[str] = None
    configuration: dict
    elementary_unique_id: str
    test_tags: List[str] = Field(default_factory=list)


class SourceFreshnessResultSchema(BaseModel):
    metadata: SourceFreshnessMetadataSchema
    test_results: DbtSourceFreshnessResultSchema


class SourceFreshnessInvocationSchema(BaseModel):
    id: str
    time_utc: str
    status: str

    @validator("time_utc", pre=True)
    def format_time_utc(cls, time_utc):
        return convert_partial_iso_format_to_full_iso_format(time_utc)


class SourceFreshnessInvocationsSchema(BaseModel):
    fail_rate: float
    totals: TotalsSchema
    invocations: List[SourceFreshnessInvocationSchema]
    description: str


class SourceFreshnessRunSchema(BaseModel):
    metadata: SourceFreshnessMetadataSchema
    test_runs: Optional[SourceFreshnessInvocationsSchema]
    test_results: DbtSourceFreshnessResultSchema
