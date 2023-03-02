from typing import List, Optional

from pydantic import BaseModel


class SelectorSchema(BaseModel):
    selector: str
    results: List[Optional[str]]
