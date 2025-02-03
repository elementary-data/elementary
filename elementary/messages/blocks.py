from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel
from typing_extensions import Literal


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
    type: str
    pass


class BaseInlineTextBlock(BaseBlock):
    pass


class TextBlock(BaseInlineTextBlock):
    type: Literal["text"] = "text"
    text: str
    style: Optional[TextStyle] = None


class LinkBlock(BaseInlineTextBlock):
    type: Literal["link"] = "link"
    text: str
    url: str


class IconBlock(BaseInlineTextBlock):
    type: Literal["icon"] = "icon"
    icon: Icon


InlineBlock = Union[TextBlock, LinkBlock, IconBlock]


class HeaderBlock(BaseBlock):
    type: Literal["header"] = "header"
    text: str


class CodeBlock(BaseBlock):
    type: Literal["code"] = "code"
    text: str


class DividerBlock(BaseBlock):
    type: Literal["divider"] = "divider"


class LineBlock(BaseBlock):
    type: Literal["line"] = "line"
    inlines: List[InlineBlock]
    sep: str = " "


class BaseLinesBlock(BaseBlock):
    lines: List[LineBlock]


class LinesBlock(BaseLinesBlock):
    type: Literal["lines"] = "lines"


class FactBlock(BaseBlock):
    type: Literal["fact"] = "fact"
    title: LineBlock
    value: LineBlock


class FactListBlock(BaseBlock):
    type: Literal["fact_list"] = "fact_list"
    facts: List[FactBlock]


class ExpandableBlock(BaseBlock):
    type: Literal["expandable"] = "expandable"
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
