import json
import re
from enum import Enum

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
    TextStyle,
    WhitespaceBlock,
)
from elementary.messages.formats.unicode import ICON_TO_UNICODE
from elementary.messages.message_body import MessageBlock, MessageBody


class TableStyle(Enum):
    TABULATE = "tabulate"
    JSON = "json"


class MarkdownFormatter:
    def __init__(self, table_style: TableStyle):
        self._table_style = table_style

    def format_icon(self, icon: Icon) -> str:
        return ICON_TO_UNICODE[icon]

    def format_text_block(self, block: TextBlock) -> str:
        if block.style == TextStyle.BOLD:
            return f"**{block.text}**"
        elif block.style == TextStyle.ITALIC:
            return f"_{block.text}_"
        else:
            return block.text

    def format_inline_block(self, block: InlineBlock) -> str:
        if isinstance(block, IconBlock):
            return self.format_icon(block.icon)
        elif isinstance(block, TextBlock):
            return self.format_text_block(block)
        elif isinstance(block, LinkBlock):
            return f"[{block.text}]({block.url})"
        elif isinstance(block, InlineCodeBlock):
            return f"`{block.code}`"
        elif isinstance(block, MentionBlock):
            return block.user
        elif isinstance(block, LineBlock):
            return self.format_line_block(block)
        elif isinstance(block, WhitespaceBlock):
            return "&nbsp;"
        else:
            raise ValueError(f"Unsupported inline block type: {type(block)}")

    def format_line_block(self, block: LineBlock) -> str:
        return block.sep.join(
            [self.format_inline_block(inline) for inline in block.inlines]
        )

    def format_lines_block(self, block: LinesBlock) -> str:
        formatted_parts = []
        for index, line_block in enumerate(block.lines):
            formatted_line = self.format_line_block(line_block)
            formatted_parts.append(formatted_line)
            is_bullet = re.match(r"^\s*[*-]", formatted_line)
            is_last = index == len(block.lines) - 1
            if not is_bullet and not is_last:
                # in markdown, single line breaks are not rendered as new lines, except for bullet lists
                # so we need to add a backslash to force a new line
                formatted_parts.append("\\")
            if not is_last:
                formatted_parts.append("\n")
        return "".join(formatted_parts)

    def format_fact_list_block(self, block: FactListBlock) -> str:
        facts = [
            f"{self.format_line_block(fact.title)}: {self.format_line_block(fact.value)}"
            for fact in block.facts
        ]
        return " | ".join(facts)

    def format_table_block(self, block: TableBlock) -> str:
        if self._table_style == TableStyle.TABULATE:
            table = tabulate(block.rows, headers=block.headers, tablefmt="simple")
            return f"```\n{table}\n```"
        elif self._table_style == TableStyle.JSON:
            dicts = [
                {header: cell for header, cell in zip(block.headers, row)}
                for row in block.rows
            ]
            return f"```\n{json.dumps(dicts, indent=2)}\n```"
        else:
            raise ValueError(f"Invalid table style: {self._table_style}")

    def format_expandable_block(self, block: ExpandableBlock) -> str:
        body = self.format_message_blocks(block.body)
        quoted_body = "\n> ".join(body.split("\n"))
        return f"> **{block.title}**\\\n> {quoted_body}"

    def format_message_block(self, block: MessageBlock) -> str:
        if isinstance(block, HeaderBlock):
            return f"# {block.text}"
        elif isinstance(block, CodeBlock):
            return f"```\n{block.text}\n```"
        elif isinstance(block, LinesBlock):
            return self.format_lines_block(block)
        elif isinstance(block, FactListBlock):
            return self.format_fact_list_block(block)
        elif isinstance(block, ExpandableBlock):
            return self.format_expandable_block(block)
        elif isinstance(block, TableBlock):
            return self.format_table_block(block)
        elif isinstance(block, DividerBlock):
            return "---"
        elif isinstance(block, ActionsBlock):
            # Actions not supported for text
            return ""
        else:
            raise ValueError(f"Unsupported message block type: {type(block)}")

    def format_message_blocks(self, blocks: list[MessageBlock]) -> str:
        if not blocks:
            return ""
        return "\n\n".join([self.format_message_block(block) for block in blocks])

    def format(self, message: MessageBody) -> str:
        return self.format_message_blocks(message.blocks)


def format_markdown(
    message: MessageBody, table_style: TableStyle = TableStyle.TABULATE
) -> str:
    formatter = MarkdownFormatter(table_style)
    return formatter.format(message)
