from typing import List, Optional, Union

from pydantic import validator

from elementary.utils.schema import ExtendedBaseModel
from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class TestResultDBRowSchema(ExtendedBaseModel):
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
    column_name: Optional[str]
    test_type: str
    test_sub_type: str
    test_results_description: Optional[str]
    owners: Optional[List[str]]
    model_owner: Optional[List[str]]
    tags: Optional[List[str]]
    meta: dict
    model_meta: dict
    test_results_query: Optional[str] = None
    other: Optional[str]
    test_name: str
    test_params: dict
    severity: str
    status: str
    test_created_at: Optional[str] = None
    days_diff: float
    invocations_rank_index: int
    sample_data: Optional[Union[dict, List]] = None
    failures: Optional[int] = None

    class Config:
        smart_union = True

    @validator("detected_at", pre=True)
    def format_detected_at(cls, detected_at):
        return convert_partial_iso_format_to_full_iso_format(detected_at)

    @validator("meta", pre=True)
    def load_meta(cls, meta):
        return cls._load_var_to_dict(meta)

    @validator("model_meta", pre=True)
    def load_model_meta(cls, model_meta):
        return cls._load_var_to_dict(model_meta)

    @validator("test_params", pre=True)
    def load_test_params(cls, test_params):
        return cls._load_var_to_dict(test_params)

    @validator("test_results_description", pre=True)
    def load_test_results_description(cls, test_results_description):
        return test_results_description.strip() if test_results_description else None

    @validator("tags", pre=True)
    def load_tags(cls, tags):
        return cls._load_var_to_list(tags)

    @validator("owners", pre=True)
    def load_owners(cls, owners):
        return cls._load_var_to_list(owners)

    @validator("model_owner", pre=True)
    def load_model_owner(cls, model_owner):
        return cls._load_var_to_list(model_owner)

    @validator("failures", pre=True)
    def parse_failures(cls, failures, values):
        test_type = values.get("test_type")
        # Elementary's tests doesn't return correct failures.
        return failures or None if test_type == "dbt_test" else None


class SourceFreshnessResultDBRowSchema(ExtendedBaseModel):
    __test__ = False  # Mark for pytest - The class name starts with "Test" which throws warnings on pytest runs

    source_freshness_execution_id: str
    unique_id: str
    max_loaded_at: Optional[str] = None
    generated_at: str
    status: str
    normalized_status: str
    error: Optional[str] = None
    invocation_id: str
    database_name: Optional[str] = None
    schema_name: str
    source_name: str
    table_name: str
    test_type: str
    test_sub_type: str
    loaded_at_field: str
    meta: dict
    owners: Optional[List[str]]
    tags: Optional[List[str]]
    error_after: dict
    warn_after: dict
    filter: Optional[str] = None
    relation_name: str
    invocations_rank_index: int
    max_loaded_at_time_ago_in_s: Optional[float] = None
    freshness_description: Optional[str] = None
    snapshotted_at: Optional[str] = None

    class Config:
        smart_union = True

    @validator("generated_at", pre=True)
    def format_generated_at(cls, generated_at):
        return convert_partial_iso_format_to_full_iso_format(generated_at)

    @validator("max_loaded_at", pre=True)
    def format_max_loaded_at(cls, max_loaded_at):
        return (
            convert_partial_iso_format_to_full_iso_format(max_loaded_at)
            if max_loaded_at
            else None
        )

    @validator("snapshotted_at", pre=True)
    def format_snapshotted_at(cls, snapshotted_at):
        return (
            convert_partial_iso_format_to_full_iso_format(snapshotted_at)
            if snapshotted_at
            else None
        )

    @validator("meta", pre=True)
    def load_meta(cls, meta):
        return cls._load_var_to_dict(meta)

    @validator("tags", pre=True)
    def load_tags(cls, tags):
        return cls._load_var_to_list(tags)

    @validator("owners", pre=True)
    def load_owners(cls, owners):
        return cls._load_var_to_list(owners)

    @validator("error_after", pre=True)
    def load_error_after(cls, error_after):
        return cls._load_var_to_dict(error_after)

    @validator("warn_after", pre=True)
    def load_warn_after(cls, warn_after):
        return cls._load_var_to_dict(warn_after)
