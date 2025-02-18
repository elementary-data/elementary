from enum import Enum
from typing import Any, Dict, List, Optional, Union

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
    primary: bool = False


class FactListBlock(BaseBlock):
    type: Literal["fact_list"] = "fact_list"
    facts: List[FactBlock]


class TableBlock(BaseBlock):
    type: Literal["table"] = "table"
    headers: List[str]
    rows: List[List[Any]]

    @classmethod
    def from_dicts(cls, data: List[Dict[str, Any]]) -> "TableBlock":
        if not data:
            return cls(headers=[], rows=[])

        headers = list(data[0].keys())
        rows = [[row.get(header) for header in headers] for row in data]
        return cls(headers=headers, rows=rows)


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
    TableBlock,
    "ExpandableBlock",
]

# Update forward references for recursive types
ExpandableBlock.update_forward_refs()
