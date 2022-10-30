from typing import List, Optional
from pydantic import BaseModel, validator

from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


TestUniqueIdType = str
ModelUniqueIdType = str


class TestMetadataSchema(BaseModel):
    id: str
    model_unique_id: Optional[ModelUniqueIdType] = None
    test_unique_id: TestUniqueIdType
    detected_at: str
    database_name: str = None
    schema_name: str
    table_name: Optional[str] = None
    column_name: Optional[str]
    test_type: str
    test_sub_type: str
    test_results_description: Optional[str]
    owners: Optional[str]
    tags: Optional[str]
    test_results_query: Optional[str] = None
    other: Optional[str]
    test_name: str
    test_params: Optional[str]
    severity: str
    status: str
    test_created_at: Optional[str] = None
    days_diff: float

    @validator("detected_at", pre=True)
    def format_detected_at(cls, detected_at):
        return convert_partial_iso_format_to_full_iso_format(detected_at)


class InvocationSchema(BaseModel):
    affected_rows: Optional[int]
    time_utc: str
    id: str
    status: str

    @validator("time_utc", pre=True)
    def format_time_utc(cls, time_utc):
        return convert_partial_iso_format_to_full_iso_format(time_utc)


class TotalsInvocationsSchema(BaseModel):
    errors: Optional[int] = 0
    warnings: Optional[int] = 0
    passed: Optional[int] = 0
    resolved: Optional[int] = 0


class InvocationsSchema(BaseModel):
    fail_rate: float
    totals: TotalsInvocationsSchema
    invocations: List[InvocationSchema]
    description: str
