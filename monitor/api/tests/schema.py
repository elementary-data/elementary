from typing import List, Optional
from pydantic import BaseModel, validator


class RawTestMetadataSchema(BaseModel):
    id: str
    model_unique_id: str
    test_unique_id: str
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
    test_rows_sample: Optional[list]
    test_runs: Optional[list]


class TestMetadataSchema(BaseModel):
    pass


class MetricSchema(BaseModel):
    pass


class InvocationSchema(BaseModel):
    pass
