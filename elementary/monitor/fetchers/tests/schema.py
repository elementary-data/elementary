from typing import List, Literal, Optional, Union

from elementary.monitor.api.models.schema import NormalizedArtifactSchema
from elementary.utils.pydantic_shim import Field, validator
from elementary.utils.schema import ExtendedBaseModel
from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class NormalizedTestSchema(NormalizedArtifactSchema):
    unique_id: str
    artifact_type: Literal["test"] = "test"


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
    test_description: Optional[str]
    original_path: str
    owners: Optional[List[str]]
    model_owner: Optional[List[str]]
    # tags is a union of test_tags and model_tags that we get from the db.
    tags: List[str] = Field(default_factory=list)
    test_tags: List[str] = Field(default_factory=list)
    meta: dict
    model_meta: dict
    model_tags: List[str] = Field(default_factory=list)
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
    package_name: Optional[str] = None
    execution_time: Optional[float] = None

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

    @validator("test_tags", pre=True)
    def load_test_tags(cls, test_tags):
        return cls._load_var_to_list(test_tags)

    @validator("model_tags", pre=True)
    def load_model_tags(cls, model_tags):
        return cls._load_var_to_list(model_tags)

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


class TestDBRowSchema(ExtendedBaseModel):
    unique_id: str
    model_unique_id: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    name: str
    description: Optional[str] = None
    package_name: Optional[str] = None
    original_path: Optional[str] = None
    test_params: dict
    meta: dict
    model_meta: dict
    tags: List[str] = Field(default_factory=list)
    model_tags: List[str] = Field(default_factory=list)
    type: str
    test_type: Optional[str]
    test_sub_type: Optional[str]
    created_at: Optional[str] = None
    latest_run_time: Optional[str] = None
    latest_run_status: Optional[str] = None

    @validator("test_params", pre=True)
    def load_test_params(cls, test_params):
        return cls._load_var_to_dict(test_params) if test_params else {}

    @validator("meta", pre=True)
    def load_meta(cls, meta):
        return cls._load_var_to_dict(meta) if meta else {}

    @validator("model_meta", pre=True)
    def load_model_meta(cls, model_meta):
        return cls._load_var_to_dict(model_meta) if model_meta else {}

    @validator("tags", pre=True)
    def load_tags(cls, tags):
        return cls._load_var_to_list(tags) if tags else []

    @validator("model_tags", pre=True)
    def load_model_tags(cls, model_tags):
        return cls._load_var_to_list(model_tags) if model_tags else []
