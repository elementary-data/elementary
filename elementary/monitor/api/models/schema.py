import os
import posixpath
from typing import Dict, List, Optional

from elementary.monitor.api.totals_schema import TotalsSchema
from elementary.monitor.fetchers.models.schema import (
    ExposureSchema,
    ModelSchema,
    SeedSchema,
    SnapshotSchema,
    SourceSchema,
)
from elementary.utils.pydantic_shim import BaseModel, Field, validator
from elementary.utils.schema import ExtendedBaseModel
from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class NormalizedArtifactSchema(ExtendedBaseModel):
    owners: Optional[List[str]] = []
    tags: Optional[List[str]] = []
    # Should be changed to artifact_name.
    # Currently, it's model_name to match the CLI UI.
    model_name: str
    normalized_full_path: str
    fqn: Optional[str] = None

    @validator("tags", pre=True)
    def load_tags(cls, tags):
        return cls._load_var_to_list(tags)

    @validator("owners", pre=True)
    def load_owners(cls, owners):
        return cls._load_var_to_list(owners)

    @validator("normalized_full_path", pre=True)
    def format_normalized_full_path_sep(cls, normalized_full_path: str) -> str:
        return posixpath.sep.join(normalized_full_path.split(os.path.sep))


# NormalizedArtifactSchema must be first in the inheritance order
class NormalizedSeedSchema(NormalizedArtifactSchema, SeedSchema):
    artifact_type: str = Field("seed", const=True)  # type: ignore  # noqa


# NormalizedArtifactSchema must be first in the inheritance order
class NormalizedSnapshotSchema(NormalizedArtifactSchema, SnapshotSchema):
    artifact_type: str = Field("snapshot", const=True)  # type: ignore  # noqa


# NormalizedArtifactSchema must be first in the inheritance order
class NormalizedModelSchema(NormalizedArtifactSchema, ModelSchema):
    artifact_type: str = Field("model", const=True)  # type: ignore  # noqa


# NormalizedArtifactSchema must be first in the inheritance order
class NormalizedSourceSchema(NormalizedArtifactSchema, SourceSchema):
    artifact_type: str = Field("source", const=True)  # type: ignore  # noqa


# NormalizedArtifactSchema must be first in the inheritance order
class NormalizedExposureSchema(NormalizedArtifactSchema, ExposureSchema):
    artifact_type: str = Field("exposure", const=True)  # type: ignore  # noqa


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

    @validator("time_utc", pre=True)
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
    compiled_code: Optional[str] = None
    last_generated_at: str
    exec_time_change_rate: float
    totals: TotalsModelRunsSchema
    runs: List[ModelRunSchema]


class ModelRunsWithTotalsSchema(BaseModel):
    runs: List[ModelRunsSchema] = list()
    totals: Dict[str, TotalsSchema] = dict()
