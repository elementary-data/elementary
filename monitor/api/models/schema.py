from typing import List, Optional
from pydantic import BaseModel, validator


class ModelSchema(BaseModel):
    name: str
    unique_id: str
    database_name: str
    schema_name: str
    table_name: str
    owners: Optional[str]
    tags: Optional[str]
    package_name: str
    description: Optional[str]
    full_path: str


class NormalizedModelSchema(ModelSchema):
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


class ModelCoverageSchema(BaseModel):
    table_tests: int
    column_tests: int
