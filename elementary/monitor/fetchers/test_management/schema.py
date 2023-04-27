from typing import List, Literal, Optional

from pydantic import BaseModel, Field, ValidationError, validator


class ResourceColumnModel(BaseModel):
    name: str
    type: str


class ResourceModel(BaseModel):
    name: str
    source_name: Optional[str]
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
    table: Optional[str]
    column: Optional[str]
    package: Optional[str] = None
    name: str
    type: Optional[str]
    args: Optional[dict]
    severity: str
    owners: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    updated_at: str
    updated_by: Optional[str]

    @validator("severity", pre=True)
    def validate_severity(cls, severity: str):
        severity_lower_string = severity.lower()
        if severity_lower_string not in ["error", "warn"]:
            raise ValidationError('Severity must be "warn" or "error"')
        return severity_lower_string


class TestsModel(BaseModel):
    tests: List[TestModel] = Field(default_factory=list)


class TagsModel(BaseModel):
    tags: List[str] = Field(default_factory=list)


class UserModel(BaseModel):
    name: str
    email: Optional[str]
    origin: Literal["account", "project"]


class UsersModel(BaseModel):
    users: List[UserModel] = Field(default_factory=list)
