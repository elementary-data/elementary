from typing import Any, Dict, List, Optional

from elementary.utils.pydantic_shim import BaseModel


class GroupItemSchema(BaseModel):
    node_id: Optional[str]
    resource_type: Optional[str]


TreeGroupSchema = Dict[str, Any]
TagsGroupSchema = Dict[str, List[GroupItemSchema]]
OwnersGroupSchema = Dict[str, List[GroupItemSchema]]


class GroupsSchema(BaseModel):
    dbt: TreeGroupSchema = dict()
    tags: TagsGroupSchema = dict()
    owners: OwnersGroupSchema = dict()
    dwh: TreeGroupSchema = dict()
    bi: Optional[TreeGroupSchema] = None
