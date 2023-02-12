from typing import Optional

from pydantic import validator

from elementary.utils.schema import ExtendedBaseModel
from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class ModelRunSchema(ExtendedBaseModel):
    unique_id: str
    invocation_id: str
    name: str
    schema: Optional[str] = None
    status: str
    execution_time: float
    full_refresh: Optional[bool] = None
    materialization: Optional[str] = None
    generated_at: str

    @validator("generated_at", pre=True)
    def format_generated_at(cls, generated_at):
        return convert_partial_iso_format_to_full_iso_format(generated_at)


class ArtifactSchema(ExtendedBaseModel):
    name: str
    unique_id: str
    owners: Optional[str]
    tags: Optional[str]
    package_name: Optional[str]
    description: Optional[str]
    full_path: str

    @validator("tags", pre=True)
    def load_tags(cls, tags):
        return cls._load_var_to_list(tags)

    @validator("owners", pre=True)
    def load_owners(cls, owners):
        return cls._load_var_to_list(owners)


class ModelSchema(ArtifactSchema):
    database_name: str = None
    schema_name: str
    table_name: str


class SourceSchema(ArtifactSchema):
    database_name: str = None
    schema_name: str
    table_name: str


class ExposureSchema(ArtifactSchema):
    url: Optional[str]
    type: Optional[str]
    maturity: Optional[str]
    owner_email: Optional[str]


class ModelTestCoverage(ExtendedBaseModel):
    model_unique_id: str
    column_tests: int = 0
    table_tests: int = 0
