import os
import posixpath
from typing import List, Optional

from pydantic import Field, validator

from elementary.utils.schema import ExtendedBaseModel
from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class ModelRunSchema(ExtendedBaseModel):
    unique_id: str
    invocation_id: str
    name: str
    schema_name: Optional[str] = Field(alias="schema", default=None)
    status: str
    execution_time: float
    full_refresh: Optional[bool] = None
    materialization: Optional[str] = None
    generated_at: str

    @validator("generated_at", pre=True)
    def format_generated_at(cls, generated_at):
        return convert_partial_iso_format_to_full_iso_format(generated_at)


class ArtifactSchema(ExtendedBaseModel):
    name: Optional[str] = None
    unique_id: Optional[str] = None
    owners: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    package_name: Optional[str] = None
    description: Optional[str] = None
    full_path: Optional[str] = None

    @validator("tags", pre=True)
    def load_tags(cls, tags):
        return cls._load_var_to_list(tags)

    @validator("owners", pre=True)
    def load_owners(cls, owners):
        return cls._load_var_to_list(owners)

    @validator("full_path", pre=True)
    def format_full_path_sep(cls, full_path: str) -> str:
        return posixpath.sep.join(full_path.split(os.path.sep))


class ModelSchema(ArtifactSchema):
    database_name: Optional[str] = None
    schema_name: str
    table_name: str
    ref_function: str = "ref"


class SourceSchema(ArtifactSchema):
    database_name: Optional[str] = None
    schema_name: str
    table_name: str
    ref_function: str = "source"


class ExposureSchema(ArtifactSchema):
    url: Optional[str] = None
    type: Optional[str] = None
    maturity: Optional[str] = None
    owner_email: Optional[str] = None
    depends_on: Optional[List[str]] = None


class ModelTestCoverage(ExtendedBaseModel):
    model_unique_id: Optional[str] = None
    column_tests: int = 0
    table_tests: int = 0
