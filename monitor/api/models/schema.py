from typing import List, Optional
from pydantic import BaseModel, validator


class NodeSchema(BaseModel):
    name: str
    unique_id: str
    owners: Optional[str]
    tags: Optional[str]
    package_name: str
    description: Optional[str]
    full_path: str


class ModelSchema(NodeSchema):
    database_name: str
    schema_name: str
    table_name: str


class ExposureSchema(NodeSchema):
    url: str
    type: str
    maturity: str
    owner_email: str


class NormalizedNodeSchema(BaseModel):
    owners: Optional[List[str]] = []
    tags: Optional[List[str]] = []
    model_name: str
    normalized_full_path: str

    @validator("owners", pre=True, always=True)
    def set_owners(cls, owners):
        return owners or []

    @validator("tags", pre=True, always=True)
    def set_tags(cls, tags):
        return tags or []


# NormalizedNodeSchema must be first in the inheritance order
class NormalizedModelSchema(NormalizedNodeSchema, ModelSchema):
    pass


# NormalizedNodeSchema must be first in the inheritance order
class NormalizedExposureSchema(NormalizedNodeSchema, ExposureSchema):
    pass


class ModelCoverageSchema(BaseModel):
    table_tests: int
    column_tests: int
