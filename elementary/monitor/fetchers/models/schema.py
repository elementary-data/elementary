import os
import posixpath
from typing import Any, Dict, List, Optional, TypeVar

from elementary.utils.pydantic_shim import Field, validator
from elementary.utils.schema import ExtendedBaseModel
from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class ModelRunSchema(ExtendedBaseModel):
    unique_id: str
    invocation_id: str
    name: str
    schema_name: Optional[str] = Field(alias="schema", default=None)
    status: str
    execution_time: float
    compiled_code: Optional[str] = None
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
    meta: Optional[Dict[str, Any]] = None
    materialization: Optional[str] = None

    @validator("tags", pre=True)
    def load_tags(cls, tags):
        return cls._load_var_to_list(tags)

    @validator("owners", pre=True)
    def load_owners(cls, owners):
        return cls._load_var_to_list(owners)

    @validator("full_path", pre=True)
    def format_full_path_sep(cls, full_path: Optional[str]) -> str:
        return posixpath.sep.join(full_path.split(os.path.sep)) if full_path else ""

    @validator("meta", pre=True)
    def load_meta(cls, meta):
        return cls._load_var_to_dict(meta)


ArtifactSchemaType = TypeVar("ArtifactSchemaType", bound=ArtifactSchema)


class SnapshotSchema(ArtifactSchema):
    database_name: Optional[str] = None
    schema_name: str
    depends_on_macros: str
    depends_on_nodes: str
    path: str
    patch_path: Optional[str]
    generated_at: str
    unique_key: Optional[str]
    incremental_strategy: Optional[str]
    table_name: str


class SeedSchema(ArtifactSchema):
    database_name: Optional[str] = None
    schema_name: str
    table_name: str


class ModelSchema(ArtifactSchema):
    database_name: Optional[str] = None
    schema_name: str
    table_name: str
    patch_path: Optional[str] = None

    def ref(self):
        return f"ref('{self.name}')"


class SourceSchema(ArtifactSchema):
    source_name: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: str
    table_name: str

    def ref(self):
        return f"source('{self.source_name}', '{self.table_name}')"


class OwnerSchema(ExtendedBaseModel):
    name: Optional[str] = None
    email: Optional[str] = None


class ExposureSchema(ArtifactSchema):
    label: Optional[str] = None
    url: Optional[str] = None
    type: Optional[str] = None
    maturity: Optional[str] = None
    depends_on_nodes: Optional[List[str]] = None
    owner: Optional[OwnerSchema] = None
    raw_queries: Optional[List[str]] = None

    @validator("depends_on_nodes", pre=True)
    def load_depends_on_nodes(cls, depends_on_nodes):
        return cls._load_var_to_list(depends_on_nodes)


class ModelTestCoverage(ExtendedBaseModel):
    model_unique_id: Optional[str] = None
    column_tests: int = 0
    table_tests: int = 0
