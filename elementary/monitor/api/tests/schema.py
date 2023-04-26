from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator

from elementary.monitor.fetchers.invocations.schema import DbtInvocationSchema
from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class ElementaryTestResultSchema(BaseModel):
    display_name: Optional[str] = None
    metrics: Optional[Union[list, dict]]
    result_description: Optional[str] = None

    class Config:
        smart_union = True


class DbtTestResultSchema(BaseModel):
    display_name: Optional[str] = None
    results_sample: Optional[list] = None
    error_message: Optional[str] = None
    failed_rows_count: Optional[int] = None


class TotalsSchema(BaseModel):
    errors: int = 0
    warnings: int = 0
    passed: int = 0
    failures: int = 0

    def add_total(self, status):
        total_adders = {
            "error": self._add_error,
            "warn": self._add_warning,
            "fail": self._add_failure,
            "pass": self._add_passed,
        }
        adder = total_adders.get(status)
        if adder:
            adder()

    def _add_error(self):
        self.errors += 1

    def _add_warning(self):
        self.warnings += 1

    def _add_passed(self):
        self.passed += 1

    def _add_failure(self):
        self.failures += 1


class InvocationSchema(BaseModel):
    affected_rows: Optional[int]
    time_utc: str
    id: str
    status: str

    @validator("time_utc", pre=True)
    def format_time_utc(cls, time_utc):
        return convert_partial_iso_format_to_full_iso_format(time_utc)


class InvocationsSchema(BaseModel):
    fail_rate: float
    totals: TotalsSchema
    invocations: List[InvocationSchema]
    description: str


class TestMetadataSchema(BaseModel):
    test_unique_id: Optional[str] = None
    elementary_unique_id: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    test_name: Optional[str] = None
    test_display_name: Optional[str] = None
    latest_run_time: Optional[str] = None
    latest_run_time_utc: Optional[str] = None
    latest_run_status: Optional[str] = None
    model_unique_id: Optional[str] = None
    table_unique_id: Optional[str] = None
    test_type: Optional[str] = None
    test_sub_type: Optional[str] = None
    test_query: Optional[str] = None
    test_params: Optional[dict] = None
    test_created_at: Optional[str] = None
    description: Optional[str] = None
    result: Optional[dict] = None
    configuration: Optional[dict] = None


class TestResultSchema(BaseModel):
    metadata: TestMetadataSchema
    test_results: Union[DbtTestResultSchema, ElementaryTestResultSchema]

    class Config:
        smart_union = True


class TestResultsWithTotalsSchema(BaseModel):
    results: Dict[Optional[str], List[TestResultSchema]] = dict()
    totals: Dict[Optional[str], TotalsSchema] = dict()
    invocation: DbtInvocationSchema = Field(default_factory=DbtInvocationSchema)


class TestRunSchema(BaseModel):
    metadata: TestMetadataSchema
    test_runs: Optional[InvocationsSchema]


class TestRunsWithTotalsSchema(BaseModel):
    runs: Dict[Optional[str], List[TestRunSchema]] = dict()
    totals: Dict[Optional[str], TotalsSchema] = dict()


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
