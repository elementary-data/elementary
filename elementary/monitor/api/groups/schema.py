from typing import Dict, List, Optional

from pydantic import BaseModel


class GroupItemSchema(BaseModel):
    node_id: Optional[str] = None
    resource_type: Optional[str] = None


DbtGroupSchema = Dict[str, dict]
TagsGroupSchema = Dict[str, List[GroupItemSchema]]
OwnersGroupSchema = Dict[str, List[GroupItemSchema]]


class GroupsSchema(BaseModel):
    dbt: DbtGroupSchema = dict()
    tags: TagsGroupSchema = dict()
    owners: OwnersGroupSchema = dict()
