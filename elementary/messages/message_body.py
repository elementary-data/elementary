from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel

from elementary.messages.blocks import (
    ActionsBlock,
    CodeBlock,
    DividerBlock,
    ExpandableBlock,
    FactListBlock,
    HeaderBlock,
    LinesBlock,
    TableBlock,
)


class Color(Enum):
    RED = "red"
    YELLOW = "yellow"
    GREEN = "green"


MessageBlock = Union[
    HeaderBlock,
    CodeBlock,
    DividerBlock,
    LinesBlock,
    FactListBlock,
    TableBlock,
    ActionsBlock,
    ExpandableBlock,
]


class MessageBody(BaseModel):
    blocks: List[MessageBlock]
    color: Optional[Color] = None
    id: Optional[str] = None


MessageBody.update_forward_refs()
