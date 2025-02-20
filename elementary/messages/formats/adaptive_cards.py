import uuid
from typing import Any, Dict, List, Optional

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
from elementary.messages.formats.html import ICON_TO_HTML
from elementary.messages.message_body import Color, MessageBlock, MessageBody

COLOR_TO_STYLE = {
    Color.RED: "Attention",
    Color.YELLOW: "Warning",
    Color.GREEN: "Good",
}


def format_icon(icon: Icon) -> str:
    return ICON_TO_HTML[icon]


def format_text_block(block: TextBlock) -> str:
    if block.style == TextStyle.BOLD:
        return f"**{block.text}**"
    elif block.style == TextStyle.ITALIC:
        return f"_{block.text}_"
    else:
        return block.text


def format_inline_block(block: InlineBlock) -> str:
    if isinstance(block, IconBlock):
        return format_icon(block.icon)
    elif isinstance(block, TextBlock):
        return format_text_block(block)
    elif isinstance(block, LinkBlock):
        return f"[{block.text}]({block.url})"
    elif isinstance(block, InlineCodeBlock):
        return block.code
    elif isinstance(block, MentionBlock):
        return block.user
    elif isinstance(block, LineBlock):
        return format_line_block_text(block)
    elif isinstance(block, WhitespaceBlock):
        return "&emsp;"
    else:
        raise ValueError(f"Unsupported inline block type: {type(block)}")


def format_line_block_text(block: LineBlock) -> str:
    return block.sep.join([format_inline_block(inline) for inline in block.inlines])


def format_line_block(block: LineBlock) -> Dict[str, Any]:
    text = format_line_block_text(block)

    return {
        "type": "TextBlock",
        "text": text,
        "wrap": True,
    }


def format_lines_block(block: LinesBlock) -> List[Dict[str, Any]]:
    return [format_line_block(line_block) for line_block in block.lines]


def format_header_block(
    block: HeaderBlock, color: Optional[Color] = None
) -> Dict[str, Any]:
    return {
        "type": "Container",
        "items": [
            {
                "type": "TextBlock",
                "text": block.text,
                "weight": "bolder",
                "size": "large",
                "wrap": True,
            }
        ],
        "style": COLOR_TO_STYLE[color] if color else "Default",
    }


def format_code_block(block: CodeBlock) -> Dict[str, Any]:
    return {
        "type": "Container",
        "style": "emphasis",
        "items": [
            {
                "type": "RichTextBlock",
                "inlines": [
                    {
                        "type": "TextRun",
                        "text": block.text,
                        "fontType": "Monospace",
                    }
                ],
            }
        ],
    }


def format_fact_list_block(block: FactListBlock) -> Dict[str, Any]:
    return {
        "type": "FactSet",
        "facts": [
            {
                "title": format_line_block_text(fact.title),
                "value": format_line_block_text(fact.value),
            }
            for fact in block.facts
        ],
    }


def format_table_block(block: TableBlock) -> Dict[str, Any]:
    return {
        "type": "Table",
        "columns": [{"width": 1} for _ in block.headers],
        "rows": [
            {
                "type": "TableRow",
                "cells": [
                    {
                        "type": "TableCell",
                        "items": [
                            {
                                "type": "TextBlock",
                                "text": str(cell),
                            }
                        ],
                    }
                    for cell in row
                ],
            }
            for row in [block.headers, *block.rows]
        ],
    }


def format_message_block(
    block: MessageBlock, color: Optional[Color] = None
) -> List[Dict[str, Any]]:
    if isinstance(block, HeaderBlock):
        return [format_header_block(block, color)]
    elif isinstance(block, CodeBlock):
        return [format_code_block(block)]
    elif isinstance(block, LinesBlock):
        return format_lines_block(block)
    elif isinstance(block, FactListBlock):
        return [format_fact_list_block(block)]
    elif isinstance(block, ExpandableBlock):
        return format_expandable_block(block)
    elif isinstance(block, TableBlock):
        return [format_table_block(block)]
    elif isinstance(block, ActionsBlock):
        # Not supported in webhooks, so we don't need to format it.
        # When we add support for teams apps, we will need to format it.
        return []
    else:
        raise ValueError(f"Unsupported message block type: {type(block)}")


def split_message_blocks_by_divider(
    blocks: List[MessageBlock],
) -> List[List[MessageBlock]]:
    first_divider_index = next(
        (i for i, block in enumerate(blocks) if isinstance(block, DividerBlock)),
        None,
    )
    if first_divider_index is None:
        return [blocks] if blocks else []
    return [
        blocks[:first_divider_index],
        *split_message_blocks_by_divider(blocks[first_divider_index + 1 :]),
    ]


def format_divided_message_blocks(
    blocks: List[MessageBlock],
    divider: bool = False,
    color: Optional[Color] = None,
) -> Dict[str, Any]:
    return {
        "type": "Container",
        "separator": divider,
        "items": [
            item for block in blocks for item in format_message_block(block, color)
        ],
    }


def format_expandable_block(block: ExpandableBlock) -> List[Dict[str, Any]]:
    block_title = block.title
    expandable_target_id = f"expandable-{uuid.uuid4()}"
    return [
        {
            "type": "ActionSet",
            "actions": [
                {
                    "type": "Action.ToggleVisibility",
                    "title": block_title,
                    "targetElements": [expandable_target_id],
                }
            ],
        },
        {
            "type": "Container",
            "id": expandable_target_id,
            "items": format_message_blocks(block.body),
            "isVisible": block.expanded,
        },
    ]


def format_message_blocks(
    blocks: List[MessageBlock], color: Optional[Color] = None
) -> List[Dict[str, Any]]:
    if not blocks:
        return []

    message_blocks = split_message_blocks_by_divider(blocks)
    # The divider is not a block in adaptive cards, it's a property of the container.
    return [
        format_divided_message_blocks(blocks, divider=True, color=color)
        for blocks in message_blocks
    ]


def format_adaptive_card_body(message: MessageBody) -> List[Dict[str, Any]]:
    return format_message_blocks(message.blocks, message.color)


def format_adaptive_card(message: MessageBody, version: str = "1.5") -> Dict[str, Any]:
    if version < "1.2" or version > "1.6":
        raise ValueError(f"Version {version} is not supported")
    return {
        "type": "AdaptiveCard",
        "body": format_adaptive_card_body(message),
        "version": version,
    }
