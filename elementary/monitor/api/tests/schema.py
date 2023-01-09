import json
import re
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, validator

from elementary.utils.time import convert_partial_iso_format_to_full_iso_format

TestUniqueIdType = str
ModelUniqueIdType = str


class ElementaryTestResultSchema(BaseModel):
    display_name: Optional[str] = None
    metrics: Optional[Union[list, dict]]
    result_description: Optional[str] = None

    # pydantic has a bug with Union fields. This is how to support it.
    class Config:
        smart_union = True


class DbtTestResultSchema(BaseModel):
    display_name: Optional[str] = None
    results_sample: Optional[list] = None
    error_message: Optional[str] = None
    failed_rows_count: Optional[int] = None


class TestMetadataSchema(BaseModel):
    id: str
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

    def get_failed_rows_count(self):
        failed_rows_count = -1
        if self.status != "pass" and self.test_results_description:
            found_rows_number = re.search(r"\d+", self.test_results_description)
            if found_rows_number:
                found_rows_number = found_rows_number.group()
                failed_rows_count = int(found_rows_number)
        return failed_rows_count

    def get_test_results(
        self, test_sample_data: Dict[str, Any]
    ) -> Union[DbtTestResultSchema, ElementaryTestResultSchema]:
        if self.test_type == "dbt_test":
            test_results = DbtTestResultSchema(
                display_name=self.test_name,
                results_sample=test_sample_data,
                error_message=self.test_results_description,
                failed_rows_count=self.get_failed_rows_count(),
            )
        else:
            test_sub_type_display_name = self.test_sub_type.replace("_", " ").title()
            if self.test_type == "anomaly_detection":
                if test_sample_data and self.test_sub_type != "dimension":
                    test_sample_data.sort(key=lambda metric: metric.get("end_time"))
                test_results = ElementaryTestResultSchema(
                    display_name=test_sub_type_display_name,
                    metrics=test_sample_data,
                    result_description=self.test_results_description,
                )
            elif self.test_type == "schema_change":
                test_results = ElementaryTestResultSchema(
                    display_name=test_sub_type_display_name.lower(),
                    result_description=self.test_results_description,
                )
        return test_results


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


class TestInfoSchema(BaseModel):
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
    metadata: TestInfoSchema
    test_results: Union[dict, list]

    # pydantic has a bug with Union fields. This is how to support it.
    class Config:
        smart_union = True


class TestRunSchema(BaseModel):
    metadata: TestInfoSchema
    test_runs: InvocationsSchema
