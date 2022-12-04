from pydantic import BaseModel
from typing import Dict, List

DbtSidebarSchema = dict
TagsSidebarSchema = Dict[str, List[str]]
OwnersSidebarSchema = Dict[str, List[str]]


class SidebarsSchema(BaseModel):
    dbt: DbtSidebarSchema
    tags: TagsSidebarSchema
    owners: OwnersSidebarSchema
