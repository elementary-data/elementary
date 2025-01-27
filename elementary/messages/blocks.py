from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel


class Icon(Enum):
    RED_TRIANGLE = "red_triangle"
    X = "x"
    WARNING = "warning"
    EXCLAMATION = "exclamation"
    CHECK = "check"
    MAGNIFYING_GLASS = "magnifying_glass"
    HAMMER_AND_WRENCH = "hammer_and_wrench"
    POLICE_LIGHT = "police_light"
    INFO = "info"
    EYE = "eye"
    GEAR = "gear"
    BELL = "bell"


class TextStyle(Enum):
    BOLD = "bold"
    ITALIC = "italic"


class BaseBlock(BaseModel):
    pass


class BaseInlineTextBlock(BaseBlock):
    pass


class TextBlock(BaseInlineTextBlock):
    text: str
    style: Optional[TextStyle] = None


class LinkBlock(BaseInlineTextBlock):
    text: str
    url: str


class IconBlock(BaseInlineTextBlock):
    icon: Icon


InlineBlock = Union[TextBlock, LinkBlock, IconBlock]


class HeaderBlock(BaseBlock):
    text: str


class CodeBlock(BaseBlock):
    text: str


class DividerBlock(BaseBlock):
    pass


class LineBlock(BaseBlock):
    inlines: List[InlineBlock]
    sep: str = " "


class BaseLinesBlock(BaseBlock):
    lines: List[LineBlock]


class LinesBlock(BaseLinesBlock):
    pass


class FactBlock(BaseBlock):
    title: LineBlock
    value: LineBlock


class FactListBlock(BaseBlock):
    facts: List[FactBlock]


class ExpandableBlock(BaseBlock):
    title: str
    body: List["InExpandableBlock"]
    expanded: bool = False


InExpandableBlock = Union[
    HeaderBlock,
    CodeBlock,
    DividerBlock,
    LinesBlock,
    FactListBlock,
    "ExpandableBlock",
]

ExpandableBlock.model_rebuild()
