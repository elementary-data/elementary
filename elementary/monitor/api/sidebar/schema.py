from typing import Dict, List, Union

from pydantic import BaseModel

DbtSidebarSchema = Dict[str, Union[list, dict]]
TagsSidebarSchema = Dict[str, List[str]]
OwnersSidebarSchema = Dict[str, List[str]]


class SidebarsSchema(BaseModel):
    dbt: DbtSidebarSchema = dict()
    tags: TagsSidebarSchema = dict()
    owners: OwnersSidebarSchema = dict()
