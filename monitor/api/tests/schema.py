from typing import List, Optional
from pydantic import BaseModel


TestUniqueIdType = str
ModelUniqueIdType = str


class TestMetadataSchema(BaseModel):
    id: str
    model_unique_id: Optional[ModelUniqueIdType] = None
    test_unique_id: TestUniqueIdType
    detected_at: str
    database_name: str
    schema_name: str
    table_name: Optional[str] = None
    column_name: Optional[str]
    test_type: str
    test_sub_type: str
    test_results_description: Optional[str]
    owners: Optional[str]
    tags: Optional[str]
    test_results_query: str
    other: Optional[str]
    test_name: str
    test_params: Optional[str]
    severity: str
    status: str
    days_diff: float
    

class InvocationSchema(BaseModel):
    affected_rows: Optional[int]
    time_utc: str
    id: str
    status: str


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
