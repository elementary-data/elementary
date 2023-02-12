import json
from typing import List, Optional, Union

from pydantic import BaseModel, validator

from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class TestResultDBRowSchema(BaseModel):
    __test__ = False  # Mark for pytest - The class name starts with "Test" which throws warnings on pytest runs

    id: str
    invocation_id: str = None
    test_execution_id: str = None
    model_unique_id: Optional[str] = None
    test_unique_id: str
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
