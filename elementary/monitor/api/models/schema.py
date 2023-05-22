import os
import posixpath
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, validator

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
class NormalizedModelSchema(NormalizedArtifactSchema, ModelSchema):
    artifact_type: str = Field("model", const=True)


# NormalizedArtifactSchema must be first in the inheritance order
class NormalizedSourceSchema(NormalizedArtifactSchema, SourceSchema):
    artifact_type: str = Field("source", const=True)


# NormalizedArtifactSchema must be first in the inheritance order
class NormalizedExposureSchema(NormalizedArtifactSchema, ExposureSchema):
    artifact_type: str = Field("exposure", const=True)


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
    errors: Optional[int] = 0
    success: Optional[int] = 0


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


class ModelRunsWithTotalsSchema(BaseModel):
    runs: List[ModelRunsSchema] = list()
    totals: Dict[str, TotalsSchema] = dict()
