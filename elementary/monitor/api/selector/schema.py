from typing import List

from pydantic import BaseModel


class SelectorSchema(BaseModel):
    selector: str
    results: List[str]
