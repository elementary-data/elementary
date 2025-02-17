from typing import Any, Dict, List, Optional, Tuple

from slack_sdk.models import blocks as slack_blocks

from elementary.messages.blocks import (
    CodeBlock,
    DividerBlock,
    ExpandableBlock,
    FactBlock,
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


class BlockKitBuilder:
    _SECONDARY_FACT_CHUNK_SIZE = 2
    _LONGEST_MARKDOWN_SUFFIX_LEN = 3  # length of markdown's code suffix (```)

    def __init__(self) -> None:
        self._blocks: List[dict] = []
        self._attachment_blocks: List[dict] = []
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

    def _format_markdown_section_text(self, text: str) -> dict:
        if len(text) > slack_blocks.SectionBlock.text_max_length:
            text = (
                text[
                    : slack_blocks.SectionBlock.text_max_length
                    - len("...")
                    - self._LONGEST_MARKDOWN_SUFFIX_LEN
                ]
                + "..."
                + text[-self._LONGEST_MARKDOWN_SUFFIX_LEN :]
            )
        return {
            "type": "mrkdwn",
            "text": text,
        }

    def _format_markdown_section(self, text: str) -> dict:
        return {
            "type": "section",
            "text": self._format_markdown_section_text(text),
        }

    def _add_block(self, block: dict) -> None:
        if not self._is_divided:
            self._blocks.append(block)
        else:
            self._attachment_blocks.append(block)

    def _add_lines_block(self, block: LinesBlock) -> None:
        formatted_lines = [
            self._format_line_block_text(line_block) for line_block in block.lines
        ]
        self._add_block(self._format_markdown_section("\n".join(formatted_lines)))

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
        self._add_block(self._format_markdown_section(f"```{block.text}```"))

    def _add_primary_fact(self, fact: FactBlock) -> None:
        self._add_block(
            self._format_markdown_section(
                f"*{self._format_line_block_text(fact.title)}*\n{self._format_line_block_text(fact.value)}"
            )
        )

    def _add_secondary_facts(self, facts: List[FactBlock]) -> None:
        if not facts:
            return
        self._add_block(
            {
                "type": "section",
                "fields": [
                    self._format_markdown_section_text(
                        f"*{self._format_line_block_text(fact.title)}*\n{self._format_line_block_text(fact.value)}"
                    )
                    for fact in facts
                ],
            }
        )

    def _add_fact_list_block(self, block: FactListBlock) -> None:
        remaining_facts = block.facts[:]
        secondary_facts: List[FactBlock] = []
        while remaining_facts:
            current_fact = remaining_facts.pop(0)
            if current_fact.primary:
                self._add_secondary_facts(secondary_facts)
                secondary_facts = []
                self._add_primary_fact(current_fact)
            else:
                if len(secondary_facts) >= self._SECONDARY_FACT_CHUNK_SIZE:
                    self._add_secondary_facts(secondary_facts)
                    secondary_facts = []
                secondary_facts.append(current_fact)
        self._add_secondary_facts(secondary_facts)

    def _add_divider_block(self, block: DividerBlock) -> None:
        self._add_block({"type": "divider"})
        self._is_divided = True

    def _add_expandable_block(self, block: ExpandableBlock) -> None:
        """
        Expandable blocks are not supported in Slack Block Kit.
        However, slack automatically collapses a large section block into an expandable block.
        """
        self._add_block(
            {
                "type": "section",
                "text": self._format_markdown_section_text(f"*{block.title}*"),
            }
        )
        self._add_message_blocks(block.body)

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

    def _get_final_blocks(
        self, color: Optional[Color]
    ) -> Tuple[List[dict], List[dict]]:
        """
        Slack does not support coloring regular messages, only attachments.
        Also, regular messages are always displayed in full, while attachments show the first 5 blocks (with a "show more" button).
        The way we handle this is as follows:
        - If we have a divider block, everything up to it and including it is a regular message, and everything after it is an attachment.
        - If we don't have a divider block:
          - If we have a color, everything is an attachment (in order to always display colored messages).
          - If we don't have a color, everything is a regular message.
        """
        if self._is_divided or not color:
            return self._blocks, self._attachment_blocks
        else:
            return [], self._blocks

    def build(self, message: MessageBody) -> Dict[str, Any]:
        self._blocks = []
        self._attachment_blocks = []
        self._add_message_blocks(message.blocks)
        color_code = COLOR_MAP.get(message.color) if message.color else None
        blocks, attachment_blocks = self._get_final_blocks(message.color)
        built_message = {
            "blocks": blocks,
            "attachments": [
                {
                    "blocks": attachment_blocks,
                }
            ],
        }
        if color_code:
            built_message["attachments"][0]["color"] = color_code
        return built_message


def format_block_kit(message: MessageBody) -> Dict[str, Any]:
    builder = BlockKitBuilder()
    return builder.build(message)
