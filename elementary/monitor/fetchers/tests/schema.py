import json
from typing import List, Optional, Union

from pydantic import BaseModel, validator

from elementary.utils.time import convert_partial_iso_format_to_full_iso_format

TestUniqueIdType = str
ModelUniqueIdType = str


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


class TestResultDBRowSchema(BaseModel):
    __test__ = False  # Mark for pytest - The class name starts with "Test" which throws warnings on pytest runs

    id: str
    invocation_id: str = None
    test_execution_id: str = None
    model_unique_id: Optional[ModelUniqueIdType] = None
    test_unique_id: TestUniqueIdType
    elementary_unique_id: str
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
    meta: Optional[dict]
    test_results_query: Optional[str] = None
    other: Optional[str]
    test_name: str
    test_params: Optional[dict]
    severity: str
    status: str
    test_created_at: Optional[str] = None
    days_diff: float
    invocations_rank_index: int
    sample_data: Optional[Union[dict, List]] = None

    class Config:
        smart_union = True

    @validator("detected_at", pre=True)
    def format_detected_at(cls, detected_at):
        return convert_partial_iso_format_to_full_iso_format(detected_at)

    @validator("meta", pre=True)
    def load_meta(cls, meta):
        return json.loads(meta) if meta else {}

    @validator("test_params", pre=True)
    def load_test_params(cls, test_params):
        return json.loads(test_params) if test_params else {}

    @validator("test_results_description", pre=True)
    def load_test_results_description(cls, test_results_description):
        return test_results_description.strip() if test_results_description else None


class TotalsSchema(BaseModel):
    errors: Optional[int] = 0
    warnings: Optional[int] = 0
    passed: Optional[int] = 0
    failures: Optional[int] = 0

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
    test_results: Optional[Union[dict, list]] = None

    class Config:
        smart_union = True


class TestRunSchema(BaseModel):
    metadata: TestMetadataSchema
    test_runs: InvocationsSchema
