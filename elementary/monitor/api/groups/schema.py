from typing import Any, Dict, List, Optional

from elementary.utils.pydantic_shim import BaseModel


class GroupItemSchema(BaseModel):
    node_id: Optional[str]
    resource_type: Optional[str]


TreeGroupSchema = Dict[str, Any]
TagsGroupSchema = Dict[str, List[GroupItemSchema]]
OwnersGroupSchema = Dict[str, List[GroupItemSchema]]


class TreeViewSchema(BaseModel):
    name: str
    data: Dict[str, Any]


class GroupsSchema(BaseModel):
    data_assets: List[TreeViewSchema] = []
    bi_assets: Optional[List[TreeViewSchema]] = None

    # deprecated
    dbt: TreeGroupSchema = dict()
    tags: TagsGroupSchema = dict()
    owners: OwnersGroupSchema = dict()
