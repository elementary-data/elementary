from typing import List, Optional, Union

from elementary.monitor.api.totals_schema import TotalsSchema
from elementary.utils.pydantic_shim import BaseModel, Field, validator
from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class ElementaryTestResultSchema(BaseModel):
    display_name: Optional[str] = None
    metrics: Optional[Union[list, dict]] = None
    result_description: Optional[str] = None

    class Config:
        smart_union = True


class DbtTestResultSchema(BaseModel):
    display_name: Optional[str] = None
    results_sample: Optional[list] = None
    error_message: Optional[str] = None
    failed_rows_count: Optional[int] = None


class InvocationSchema(BaseModel):
    affected_rows: Optional[int]
    time_utc: str
    id: str
    status: str
    execution_time: Optional[float] = None

    @validator("time_utc", pre=True)
    def format_time_utc(cls, time_utc):
        return convert_partial_iso_format_to_full_iso_format(time_utc)


class InvocationsSchema(BaseModel):
    fail_rate: float
    totals: TotalsSchema
    invocations: List[InvocationSchema]
    description: str


class TestMetadataSchema(BaseModel):
    test_unique_id: str
    elementary_unique_id: str
    database_name: Optional[str] = None
    schema_name: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    test_name: str
    test_display_name: str
    original_path: Optional[str] = None
    latest_run_time: str
    latest_run_time_utc: str
    latest_run_status: str
    model_unique_id: Optional[str] = None
    table_unique_id: Optional[str] = None
    test_type: str
    test_sub_type: str
    test_query: Optional[str] = None
    test_params: dict
    test_created_at: Optional[str] = None
    description: Optional[str] = None
    result: dict
    configuration: dict
    test_tags: List[str] = Field(default_factory=list)
    normalized_full_path: Optional[str] = None


class TestSchema(BaseModel):
    unique_id: str
    model_unique_id: Optional[str] = None
    table_unique_id: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    name: str
    display_name: str
    original_path: Optional[str] = None
    type: str
    test_type: Optional[str] = None
    test_sub_type: Optional[str] = None
    test_params: dict
    description: Optional[str] = None
    configuration: dict
    tags: List[str] = Field(default_factory=list)
    normalized_full_path: Optional[str] = None
    created_at: Optional[str] = None
    latest_run_time: Optional[str] = None
    latest_run_time_utc: Optional[str] = None
    latest_run_status: Optional[str] = None


class TestResultSchema(BaseModel):
    metadata: TestMetadataSchema
    test_results: Union[DbtTestResultSchema, ElementaryTestResultSchema]

    class Config:
        smart_union = True


class TestRunSchema(BaseModel):
    metadata: TestMetadataSchema
    test_runs: Optional[InvocationsSchema]
    median_exec_time: Optional[float]
    last_exec_time: Optional[float]


class TestResultSummarySchema(BaseModel):
    __test__ = False  # Mark for pytest - The class name starts with "Test" which throws warnings on pytest runs

    test_unique_id: str
    elementary_unique_id: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    test_type: str
    test_sub_type: str
    owners: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    subscribers: Optional[List[str]] = None
    description: Optional[str] = None
    test_name: str
    status: str
    results_counter: Optional[int] = None
