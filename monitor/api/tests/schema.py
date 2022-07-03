from typing import Optional
from pydantic import BaseModel


TestUniqueIdType = str
ModelUniqueIdType = str


class TestMetadataSchema(BaseModel):
    id: str
    model_unique_id: ModelUniqueIdType
    test_unique_id: TestUniqueIdType
    detected_at: str
    database_name: str
    schema_name: str
    table_name: str
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
