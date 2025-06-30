import json
from enum import Enum
from typing import List

from tabulate import tabulate

from elementary.messages.blocks import (
    ActionsBlock,
    CodeBlock,
    DividerBlock,
    ExpandableBlock,
    FactListBlock,
    HeaderBlock,
    Icon,
    IconBlock,
    InlineBlock,
    InlineCodeBlock,
    LineBlock,
    LinesBlock,
    LinkBlock,
    MentionBlock,
    TableBlock,
    TextBlock,
    WhitespaceBlock,
)
from elementary.messages.formats.unicode import ICON_TO_UNICODE
from elementary.messages.message_body import MessageBlock, MessageBody


class IconStyle(Enum):
    UNICODE = "unicode"
    NAME = "name"
    OMIT = "omit"


class TableStyle(Enum):
    TABULATE = "tabulate"
    JSON = "json"


class TextFormatter:
    def __init__(self, icon_style: IconStyle, table_style: TableStyle):
        self._icon_style = icon_style
        self._table_style = table_style

    def format_icon(self, icon: Icon) -> str:
        if self._icon_style == IconStyle.OMIT:
            return ""
        elif self._icon_style == IconStyle.UNICODE:
            return ICON_TO_UNICODE[icon]
        elif self._icon_style == IconStyle.NAME:
            return f":{icon.value}:"
        else:
            raise ValueError(f"Invalid icon style: {self._icon_style}")

    def format_inline_block(self, block: InlineBlock) -> str:
        if isinstance(block, IconBlock):
            return self.format_icon(block.icon)
        elif isinstance(block, TextBlock):
            return block.text
        elif isinstance(block, LinkBlock):
            return f"{block.text} ({block.url})"
        elif isinstance(block, InlineCodeBlock):
            return block.code
        elif isinstance(block, MentionBlock):
            return block.user
        elif isinstance(block, LineBlock):
            return self.format_line_block(block)
        elif isinstance(block, WhitespaceBlock):
            return " "
        else:
            raise ValueError(f"Unsupported inline block type: {type(block)}")

    def format_line_block(self, block: LineBlock) -> str:
        return block.sep.join(
            [self.format_inline_block(inline) for inline in block.inlines]
        )

    def format_lines_block(self, block: LinesBlock) -> str:
        return "\n".join(
            [self.format_line_block(line_block) for line_block in block.lines]
        )

    def format_fact_list_block(self, block: FactListBlock) -> str:
        facts = [
            f"{self.format_line_block(fact.title)}: {self.format_line_block(fact.value)}"
            for fact in block.facts
        ]
        return " | ".join(facts)

    def format_table_block(self, block: TableBlock) -> str:
        if self._table_style == TableStyle.TABULATE:
            return tabulate(block.rows, headers=block.headers, tablefmt="simple")
        elif self._table_style == TableStyle.JSON:
            dicts = [
                {header: cell for header, cell in zip(block.headers, row)}
                for row in block.rows
            ]
            return json.dumps(dicts, indent=2)
        else:
            raise ValueError(f"Invalid table style: {self._table_style}")

    def format_expandable_block(self, block: ExpandableBlock) -> str:
        return f"{block.title}\n{self.format_message_blocks(block.body)}"

    def format_message_block(self, block: MessageBlock) -> str:
        if isinstance(block, (HeaderBlock, CodeBlock)):
            return block.text
        elif isinstance(block, LinesBlock):
            return self.format_lines_block(block)
        elif isinstance(block, FactListBlock):
            return self.format_fact_list_block(block)
        elif isinstance(block, ExpandableBlock):
            return self.format_expandable_block(block)
        elif isinstance(block, TableBlock):
            return self.format_table_block(block)
        elif isinstance(block, ActionsBlock):
            # Actions not supported for text
            return ""
        elif isinstance(block, DividerBlock):
            return "--------------------------------"
        else:
            raise ValueError(f"Unsupported message block type: {type(block)}")

    def format_message_blocks(self, blocks: List[MessageBlock]) -> str:
        if not blocks:
            return ""
        return "\n".join([self.format_message_block(block) for block in blocks])

    def format(self, message: MessageBody) -> str:
        return self.format_message_blocks(message.blocks)


def format_text(
    message: MessageBody,
    icon_style: IconStyle = IconStyle.UNICODE,
    table_style: TableStyle = TableStyle.TABULATE,
) -> str:
    formatter = TextFormatter(icon_style, table_style)
    return formatter.format(message)
