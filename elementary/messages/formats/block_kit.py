from typing import Any, Dict, List

from slack_sdk.models import blocks as slack_blocks

from elementary.messages.blocks import (
    CodeBlock,
    DividerBlock,
    ExpandableBlock,
    FactListBlock,
    HeaderBlock,
    Icon,
    IconBlock,
    InlineBlock,
    LineBlock,
    LinesBlock,
    LinkBlock,
    TextBlock,
    TextStyle,
)
from elementary.messages.formats.html import ICON_TO_HTML
from elementary.messages.message_body import Color, MessageBlock, MessageBody

COLOR_MAP = {
    Color.RED: "#ff0000",
    Color.YELLOW: "#ffcc00",
    Color.GREEN: "#33b989",
}


# todo - size limits


class BlockKitBuilder:
    _FACT_CHUNK_SIZE = 2

    def __init__(self) -> None:
        self._blocks: list[dict] = []
        self._attachment_blocks: list[dict] = []
        self._is_divided = False

    def _format_icon(self, icon: Icon) -> str:
        return ICON_TO_HTML[icon]

    def _format_text_block(self, block: TextBlock) -> str:
        if block.style == TextStyle.BOLD:
            return f"*{block.text}*"
        elif block.style == TextStyle.ITALIC:
            return f"_{block.text}_"
        else:
            return block.text

    def _format_inline_block(self, block: InlineBlock) -> str:
        if isinstance(block, IconBlock):
            return self._format_icon(block.icon)
        elif isinstance(block, TextBlock):
            return self._format_text_block(block)
        elif isinstance(block, LinkBlock):
            return f"<{block.url}|{block.text}>"
        else:
            raise ValueError(f"Unsupported inline block type: {type(block)}")

    def _format_line_block_text(self, block: LineBlock) -> str:
        return block.sep.join(
            [self._format_inline_block(inline) for inline in block.inlines]
        )

    def _add_block(self, block: dict) -> None:
        if not self._is_divided:
            self._blocks.append(block)
        else:
            self._attachment_blocks.append(block)

    def _add_lines_block(self, block: LinesBlock) -> None:
        formatted_lines = [
            self._format_line_block_text(line_block) for line_block in block.lines
        ]
        self._add_block(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\n".join(formatted_lines),
                },
            }
        )

    def _add_header_block(self, block: HeaderBlock) -> None:
        if len(block.text) > slack_blocks.HeaderBlock.text_max_length:
            text = block.text[: slack_blocks.HeaderBlock.text_max_length - 3] + "..."
        else:
            text = block.text
        self._add_block(
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": text,
                },
            }
        )

    def _add_code_block(self, block: CodeBlock) -> None:
        self._add_block(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```{block.text}```",
                },
            }
        )

    def _add_fact_list_block(self, block: FactListBlock) -> None:
        if len(block.facts) == 1:
            fact = block.facts[0]
            self._add_block(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{self._format_line_block_text(fact.title)}*\n{self._format_line_block_text(fact.value)}",
                    },
                }
            )
            return
        chunked_facts = [
            block.facts[i : i + self._FACT_CHUNK_SIZE]
            for i in range(0, len(block.facts), self._FACT_CHUNK_SIZE)
        ]
        for chunk in chunked_facts:
            self._add_block(
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*{self._format_line_block_text(fact.title)}*\n{self._format_line_block_text(fact.value)}",
                        }
                        for fact in chunk
                    ],
                }
            )

    def _add_divider_block(self, block: DividerBlock) -> None:
        self._add_block({"type": "divider"})
        self._is_divided = True

    def _add_expandable_block(self, block: ExpandableBlock) -> None:
        self._add_block(
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*{block.title}*",
                    },
                ],
            }
        )
        for sub_block in block.body:
            self._add_message_block(sub_block)

    def _add_message_block(self, block: MessageBlock) -> None:
        if isinstance(block, HeaderBlock):
            self._add_header_block(block)
        elif isinstance(block, CodeBlock):
            self._add_code_block(block)
        elif isinstance(block, LinesBlock):
            self._add_lines_block(block)
        elif isinstance(block, FactListBlock):
            self._add_fact_list_block(block)
        elif isinstance(block, DividerBlock):
            self._add_divider_block(block)
        elif isinstance(block, ExpandableBlock):
            self._add_expandable_block(block)
        else:
            raise ValueError(f"Unsupported message block type: {type(block)}")

    def _add_message_blocks(self, blocks: List[MessageBlock]) -> None:
        for block in blocks:
            self._add_message_block(block)

    def _set_color(self, color: Color) -> None:
        color_code = COLOR_MAP[color]
        for attachment in self._attachment_blocks:
            attachment["color"] = color_code

    def build(self, message: MessageBody) -> Dict[str, Any]:
        self._blocks = []
        self._attachment_blocks = []
        self._add_message_blocks(message.blocks)
        color_code = COLOR_MAP.get(message.color) if message.color else None
        if self._is_divided:
            return {
                "blocks": self._blocks,
                "attachments": [
                    {
                        "blocks": self._attachment_blocks,
                        "color": color_code,
                    }
                ],
            }
        else:
            return {
                "blocks": [],
                "attachments": [
                    {
                        "blocks": self._blocks,
                        "color": color_code,
                    }
                ],
            }


def format_block_kit(message: MessageBody) -> Dict[str, Any]:
    builder = BlockKitBuilder()
    return builder.build(message)


# from elementary.messages.block_builders import BulletListBlock
# from elementary.messages.blocks import (
#     CodeBlock,
#     DividerBlock,
#     ExpandableBlock,
#     FactBlock,
#     FactListBlock,
#     HeaderBlock,
#     Icon,
#     IconBlock,
#     LineBlock,
#     LinesBlock,
#     LinkBlock,
#     TextBlock,
#     TextStyle,
# )
# from elementary.messages.formats.adaptive_cards import format_adaptive_card
# from elementary.messages.message_body import Color, MessageBody

# message_body = MessageBody(
#     blocks=[
#         HeaderBlock(text="Main Header"),
#         LinesBlock(
#             lines=[
#                 LineBlock(
#                     inlines=[
#                         TextBlock(text="Normal text"),
#                         TextBlock(text="Bold text", style=TextStyle.BOLD),
#                         TextBlock(text="Italic text", style=TextStyle.ITALIC),
#                     ]
#                 )
#             ]
#         ),
#         BulletListBlock(
#             icon="-",
#             lines=[
#                 LineBlock(inlines=[TextBlock(text="First bullet point")]),
#                 LineBlock(inlines=[TextBlock(text="Second bullet point")]),
#             ],
#         ),
#         BulletListBlock(
#             icon=Icon.CHECK,
#             lines=[LineBlock(inlines=[TextBlock(text="Check item")])],
#         ),
#         FactListBlock(
#             facts=[
#                 FactBlock(
#                     title=LineBlock(inlines=[TextBlock(text="Status")]),
#                     value=LineBlock(inlines=[TextBlock(text="Passed")]),
#                 ),
#                 FactBlock(
#                     title=LineBlock(inlines=[TextBlock(text="Tags")]),
#                     value=LineBlock(inlines=[TextBlock(text="test, example")]),
#                 ),
#             ]
#         ),
#         ExpandableBlock(
#             title="Show Details",
#             body=[
#                 LinesBlock(
#                     lines=[
#                         LineBlock(
#                             inlines=[
#                                 IconBlock(icon=Icon.MAGNIFYING_GLASS),
#                                 TextBlock(text="Details Section", style=TextStyle.BOLD),
#                             ]
#                         ),
#                         LineBlock(
#                             inlines=[
#                                 TextBlock(text="Here's some content with a"),
#                                 LinkBlock(text="link", url="https://example.com"),
#                             ]
#                         ),
#                     ]
#                 )
#             ],
#             expanded=False,
#         ),
#     ],
#     color=Color.RED,
# )

# d = format_block_kit(message_body)
# import json

# print(json.dumps(d, indent=2))
