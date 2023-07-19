import os
import posixpath
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from elementary.monitor.api.totals_schema import TotalsSchema
from elementary.monitor.fetchers.models.schema import (
    ExposureSchema,
    ModelSchema,
    SourceSchema,
)
from elementary.utils.schema import ExtendedBaseModel
from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class NormalizedArtifactSchema(ExtendedBaseModel):
    owners: Optional[List[str]] = []
    tags: Optional[List[str]] = []
    # Should be changed to artifact_name.
    # Currently its model_name to match the CLI UI.
    model_name: str
    normalized_full_path: str
    fqn: str

    @field_validator("tags", mode="before")
    def load_tags(cls, tags):
        return cls._load_var_to_list(tags)

    @field_validator("owners", mode="before")
    def load_owners(cls, owners):
        return cls._load_var_to_list(owners)

    @field_validator("normalized_full_path", mode="before")
    def format_normalized_full_path_sep(cls, normalized_full_path: str) -> str:
        return posixpath.sep.join(normalized_full_path.split(os.path.sep))


# NormalizedArtifactSchema must be first in the inheritance order
class NormalizedModelSchema(NormalizedArtifactSchema, ModelSchema):
    artifact_type: Literal["model"] = "model"


# NormalizedArtifactSchema must be first in the inheritance order
class NormalizedSourceSchema(NormalizedArtifactSchema, SourceSchema):
    artifact_type: Literal["source"] = "source"


# NormalizedArtifactSchema must be first in the inheritance order
class NormalizedExposureSchema(NormalizedArtifactSchema, ExposureSchema):
    artifact_type: Literal["exposure"] = "exposure"


class ModelCoverageSchema(BaseModel):
    table_tests: int
    column_tests: int


class ModelRunSchema(BaseModel):
    id: str
    time_utc: str
    status: str
    full_refresh: Optional[bool]
    materialization: Optional[str]
    execution_time: float

    @field_validator("time_utc", mode="before")
    def format_time_utc(cls, time_utc):
        return convert_partial_iso_format_to_full_iso_format(time_utc)


class TotalsModelRunsSchema(BaseModel):
    errors: int = 0
    success: int = 0


class ModelRunsSchema(BaseModel):
    unique_id: str
    # schema is a saved name, so we use alias
    schema_name: Optional[str] = Field(alias="schema")
    name: str
    status: str
    last_exec_time: float
    median_exec_time: float
    exec_time_change_rate: float
    totals: TotalsModelRunsSchema
    runs: List[ModelRunSchema]


class ModelRunsWithTotalsSchema(BaseModel):
    runs: List[ModelRunsSchema] = list()
    totals: Dict[str, TotalsSchema] = dict()
