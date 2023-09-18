from typing import List, Literal, Optional

from pydantic import BaseModel, Field, validator

from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class ResourceColumnModel(BaseModel):
    name: str
    type: str


class ResourceModel(BaseModel):
    id: str
    name: str
    source_name: Optional[str] = None
    db_schema: str = Field(alias="schema")
    tags: List[str] = Field(default_factory=list)
    owners: List[str] = Field(default_factory=list)
    columns: List[ResourceColumnModel] = Field(default_factory=list)


class ResourcesModel(BaseModel):
    models: List[ResourceModel] = Field(default_factory=list)
    sources: List[ResourceModel] = Field(default_factory=list)


class TestModel(BaseModel):
    id: str
    db_schema: str = Field(alias="schema")
    table: Optional[str] = None
    source_name: Optional[str] = None
    column: Optional[str] = None
    package: Optional[str] = None
    name: str
    type: Optional[str] = None
    args: Optional[dict] = None
    severity: str
    owners: List[str] = Field(default_factory=list)
    model_owners: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    model_tags: List[str] = Field(default_factory=list)
    meta: dict = Field(default_factory=dict)
    description: Optional[str]
    is_singular: bool
    updated_at: str
    updated_by: Optional[str] = None

    @validator("severity", pre=True)
    def validate_severity(cls, severity: str) -> str:
        severity_lower_string = severity.lower()
        if severity_lower_string not in ["error", "warn"]:
            raise ValueError('Severity must be "warn" or "error"')
        return severity_lower_string

    @validator("updated_at", pre=True)
    def validate_updated_at(cls, updated_at: str) -> str:
        return convert_partial_iso_format_to_full_iso_format(updated_at)


class TestsModel(BaseModel):
    tests: List[TestModel] = Field(default_factory=list)


class TagsModel(BaseModel):
    tags: List[str] = Field(default_factory=list)


class UserModel(BaseModel):
    name: str
    email: Optional[str] = None
    origin: Literal["account", "project"]


class UsersModel(BaseModel):
    users: List[UserModel] = Field(default_factory=list)
