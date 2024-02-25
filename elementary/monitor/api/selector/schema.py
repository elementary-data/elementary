from typing import List

from elementary.utils.pydantic_shim import BaseModel


class SelectorSchema(BaseModel):
    selector: str
    results: List[str]
