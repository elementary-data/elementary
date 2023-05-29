from typing import Dict, List, Union

from pydantic import BaseModel

DbtGroupSchema = Dict[str, Union[list, dict]]
TagsGroupSchema = Dict[str, List[dict]]
OwnersGroupSchema = Dict[str, List[dict]]


class GroupsSchema(BaseModel):
    dbt: DbtGroupSchema = dict()
    tags: TagsGroupSchema = dict()
    owners: OwnersGroupSchema = dict()
