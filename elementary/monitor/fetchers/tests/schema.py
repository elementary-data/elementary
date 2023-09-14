from typing import List, Optional, Union

from pydantic import ConfigDict, FieldValidationInfo, field_validator

from elementary.utils.schema import ExtendedBaseModel
from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class TestResultDBRowSchema(ExtendedBaseModel):
    model_config = ConfigDict(protected_namespaces=())
    __test__ = False  # Mark for pytest - The class name starts with "Test" which throws warnings on pytest runs

    id: str
    invocation_id: Optional[str] = None
    test_execution_id: Optional[str] = None
    model_unique_id: Optional[str] = None
    test_unique_id: str
    elementary_unique_id: str
    detected_at: str
    database_name: Optional[str] = None
    schema_name: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    test_type: str
    test_sub_type: str
    test_results_description: Optional[str] = None
    owners: Optional[List[str]] = None
    model_owner: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    meta: dict
    model_meta: dict
    test_results_query: Optional[str] = None
    other: Optional[str] = None
    test_name: str
    test_params: dict
    severity: str
    status: str
    test_created_at: Optional[str] = None
    days_diff: float
    invocations_rank_index: int
    sample_data: Optional[Union[dict, List]] = None
    failures: Optional[int] = None

    @field_validator("detected_at", mode="before")
    def format_detected_at(cls, detected_at):
        return convert_partial_iso_format_to_full_iso_format(detected_at)

    @field_validator("meta", mode="before")
    def load_meta(cls, meta):
        return cls._load_var_to_dict(meta)

    @field_validator("model_meta", mode="before")
    def load_model_meta(cls, model_meta):
        return cls._load_var_to_dict(model_meta)

    @field_validator("test_params", mode="before")
    def load_test_params(cls, test_params):
        return cls._load_var_to_dict(test_params)

    @field_validator("test_results_description", mode="before")
    def load_test_results_description(cls, test_results_description):
        return test_results_description.strip() if test_results_description else None

    @field_validator("tags", mode="before")
    def load_tags(cls, tags):
        return cls._load_var_to_list(tags)

    @field_validator("owners", mode="before")
    def load_owners(cls, owners):
        return cls._load_var_to_list(owners)

    @field_validator("model_owner", mode="before")
    def load_model_owner(cls, model_owner):
        return cls._load_var_to_list(model_owner)

    @field_validator("failures", mode="before")
    def parse_failures(cls, failures, info: FieldValidationInfo):
        test_type = info.data.get("test_type")
        # Elementary's tests doesn't return correct failures.
        return failures or None if test_type == "dbt_test" else None
