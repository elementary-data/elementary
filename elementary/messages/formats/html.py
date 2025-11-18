from __future__ import annotations

from html import escape
from typing import Sequence

from elementary.messages.blocks import (
    ActionsBlock,
    CodeBlock,
    DividerBlock,
    ExpandableBlock,
    FactBlock,
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
from elementary.messages.message_body import Color, MessageBlock, MessageBody

COLOR_MAP = {
    Color.RED: "#ff0000",
    Color.YELLOW: "#ffcc00",
    Color.GREEN: "#33b989",
}


class HTMLFormatter:
    # Container styles
    _CONTAINER_STYLES = [
        "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif",
        "font-size:14px",
        "line-height:1.5",
        "color:#1f2937",
        "background-color:#ffffff",
        "border:1px solid #e5e7eb",
        "border-radius:6px",
        "padding:24px 32px 32px 32px",
        "max-width:800px",
    ]

    # Common margin/padding styles
    _SECTION_MARGIN = "margin:0 0 12px"
    _LINE_MARGIN = "margin:0;"
    _LIST_ITEM_MARGIN = "margin:0 0 4px;"
    _FACT_LIST_MARGIN = "margin:8px 0 20px"

    # Header styles
    _HEADER_STYLE = "margin:0;font-size:18px;line-height:1.4;"

    # Code block styles
    _CODE_BLOCK_STYLE = (
        "margin:0;padding:12px;"
        "background-color:#f8fafc;border-radius:4px;"
        "font-family:'SFMono-Regular',Consolas,'Liberation Mono',Menlo,monospace;"
        "font-size:13px;line-height:1.5;white-space:pre-wrap;"
        "max-height:400px;overflow-y:auto;"
    )

    # Inline code styles
    _INLINE_CODE_STYLE = (
        "font-family:'SFMono-Regular',Consolas,'Liberation Mono',Menlo,monospace;"
        "background-color:#eef2ff;border-radius:3px;padding:1px 4px;font-size:12px;"
    )

    # Link styles
    _LINK_STYLE = "color:#2563eb;text-decoration:none;"
    _MENTION_STYLE = "color:#0ea5e9;"
    _ICON_MARGIN = "margin-right:4px;"

    # Divider styles
    _DIVIDER_STYLE = "border:none;border-top:1px solid #e5e7eb;margin:16px 0;"

    # List styles
    _UL_STYLE = "margin:0;padding-left:24px;list-style-position:outside;"
    _LIST_ITEM_ICON_STYLE = "margin:0 0 4px;list-style:none;"
    _BULLET_ICON_WRAPPER_STYLE = "margin-right:6px;"

    # Table styles
    _TABLE_STYLE = (
        "width:100%;border-collapse:collapse;margin:0 0 12px;"
        "border:1px solid #e5e7eb;border-radius:6px;overflow:hidden;"
    )
    _TABLE_HEADER_STYLE = (
        "text-align:left;padding:8px;border-bottom:1px solid #e5e7eb;"
        "font-weight:600;font-size:14px;background-color:#f8fafc;"
    )
    _TABLE_CELL_STYLE = (
        "padding:8px;border-bottom:1px solid #f3f4f6;"
        "vertical-align:top;font-size:14px;"
    )

    # Fact list styles
    _FACT_LIST_TABLE_STYLE = "width:100%;border-collapse:collapse;"
    _FACT_TITLE_STYLE = (
        "padding:4px 12px;font-weight:600;font-size:14px;color:#111827;"
        "width:160px;white-space:nowrap;"
    )

    # Expandable block styles
    _EXPANDABLE_CONTAINER_STYLE = (
        "border:1px solid #e5e7eb;border-radius:6px;margin:16px 0;"
    )
    _EXPANDABLE_SUMMARY_STYLE = (
        "padding:12px 16px;font-weight:600;background-color:#f8fafc;"
        "cursor:pointer;font-size:14px;user-select:none;-webkit-user-select:none;"
        "list-style:none;"
    )
    _EXPANDABLE_ARROW_STYLE = (
        "display:inline-block;margin-right:8px;color:#6b7280;font-size:10px;"
        "transition:transform 0.2s ease;"
    )
    _EXPANDABLE_BODY_STYLE = "padding:12px 16px;border-top:1px solid #e5e7eb;"
    _EXPANDABLE_CSS = (
        "<style>"
        "summary::-webkit-details-marker{display:none;}"
        "details[open] > summary > span{transform:rotate(90deg);}"
        "</style>"
    )

    def format(self, message: MessageBody) -> str:
        body_html = self.format_message_blocks(message.blocks)
        container_style = self._build_container_style(message.color)
        return f'<div style="{container_style}">{body_html}</div>'

    def format_message_blocks(self, blocks: Sequence[MessageBlock]) -> str:
        rendered: list[str] = []
        for block in blocks:
            formatted = self.format_message_block(block)
            if formatted:
                rendered.append(formatted)
        return "".join(rendered)

    def format_message_block(self, block: MessageBlock | ExpandableBlock) -> str:
        if isinstance(block, HeaderBlock):
            return self._format_header_block(block)
        elif isinstance(block, CodeBlock):
            return self._format_code_block(block)
        elif isinstance(block, LinesBlock):
            return self._format_lines_block(block)
        elif isinstance(block, FactListBlock):
            return self._format_fact_list_block(block)
        elif isinstance(block, TableBlock):
            return self._format_table_block(block)
        elif isinstance(block, ExpandableBlock):
            return self._format_expandable_block(block)
        elif isinstance(block, DividerBlock):
            return self._format_divider_block()
        elif isinstance(block, ActionsBlock):
            # Not supported in HTML emails (no interactivity without JavaScript)
            return ""
        else:
            raise ValueError(f"Unsupported message block type: {type(block)}")

    def _format_header_block(self, block: HeaderBlock) -> str:
        return self._wrap_section(
            f'<h1 style="{self._HEADER_STYLE}">{escape(block.text)}</h1>'
        )

    def _format_code_block(self, block: CodeBlock) -> str:
        code_html = escape(block.text)
        return self._wrap_section(
            f'<pre style="{self._CODE_BLOCK_STYLE}">{code_html}</pre>'
        )

    def _format_divider_block(self) -> str:
        return f'<hr style="{self._DIVIDER_STYLE}" />'

    def _wrap_section(self, html: str) -> str:
        return f'<div style="{self._SECTION_MARGIN}">{html}</div>'

    def _format_icon(self, icon: Icon) -> str:
        return (
            f'<span style="{self._ICON_MARGIN}">{escape(ICON_TO_UNICODE[icon])}</span>'
        )

    def _format_text_block(self, block: TextBlock) -> str:
        text = escape(block.text)
        if block.style == TextStyle.BOLD:
            return f"<strong>{text}</strong>"
        elif block.style == TextStyle.ITALIC:
            return f"<em>{text}</em>"
        else:
            return text

    def _format_inline_block(self, block: InlineBlock) -> str:
        if isinstance(block, IconBlock):
            return self._format_icon(block.icon)
        elif isinstance(block, TextBlock):
            return self._format_text_block(block)
        elif isinstance(block, LinkBlock):
            return self._format_link_block(block)
        elif isinstance(block, InlineCodeBlock):
            return self._format_inline_code_block(block)
        elif isinstance(block, MentionBlock):
            return self._format_mention_block(block)
        elif isinstance(block, LineBlock):
            return self._format_line_block(block)
        elif isinstance(block, WhitespaceBlock):
            return "&nbsp;"
        else:
            raise ValueError(f"Unsupported inline block type: {type(block)}")

    def _format_link_block(self, block: LinkBlock) -> str:
        url = escape(block.url, quote=True)
        text = escape(block.text)
        return (
            f'<a style="{self._LINK_STYLE}" '
            f'href="{url}" target="_blank" rel="noopener noreferrer">{text}</a>'
        )

    def _format_inline_code_block(self, block: InlineCodeBlock) -> str:
        return f'<code style="{self._INLINE_CODE_STYLE}">{escape(block.code)}</code>'

    def _format_mention_block(self, block: MentionBlock) -> str:
        return f'<span style="{self._MENTION_STYLE}">{escape(block.user)}</span>'

    def _format_line_block(self, block: LineBlock) -> str:
        inlines = [self._format_inline_block(inline) for inline in block.inlines]
        separator = escape(block.sep, quote=False)
        return separator.join(inlines)

    def _format_lines_block(self, block: LinesBlock) -> str:
        if not block.lines:
            return ""

        if self._is_bullet_list(block):
            return self._format_as_bullet_list(block)

        lines_html = [
            f'<div style="{self._LINE_MARGIN}">{self._format_line_block(line_block)}</div>'
            for line_block in block.lines
        ]
        return self._wrap_section("".join(lines_html))

    def _skip_whitespace(self, inlines: Sequence[InlineBlock], start_idx: int) -> int:
        idx = start_idx
        while idx < len(inlines) and isinstance(inlines[idx], WhitespaceBlock):
            idx += 1
        return idx

    def _is_bullet_marker(self, inline: InlineBlock) -> bool:
        if isinstance(inline, IconBlock):
            return True
        if isinstance(inline, TextBlock) and len(inline.text) <= 2:
            return True
        return False

    def _is_bullet_list(self, block: LinesBlock) -> bool:
        if not block.lines:
            return False

        for line in block.lines:
            if len(line.inlines) < 3:
                return False

            idx = self._skip_whitespace(line.inlines, 0)
            if idx >= len(line.inlines):
                return False

            if not self._is_bullet_marker(line.inlines[idx]):
                return False

        return True

    def _extract_bullet_parts(
        self, line: LineBlock
    ) -> tuple[InlineBlock, Sequence[InlineBlock]]:
        idx = self._skip_whitespace(line.inlines, 0)

        bullet_inline = line.inlines[idx]
        idx += 1

        if idx < len(line.inlines):
            space_inline = line.inlines[idx]
            if isinstance(space_inline, TextBlock) and space_inline.text == " ":
                idx += 1

        content_inlines = line.inlines[idx:]
        return bullet_inline, content_inlines

    def _format_bullet_list_item(
        self, bullet_inline: InlineBlock, content_inlines: Sequence[InlineBlock]
    ) -> str:
        content_html = "".join(
            self._format_inline_block(inline) for inline in content_inlines
        )

        if isinstance(bullet_inline, IconBlock):
            bullet_html = self._format_icon(bullet_inline.icon)
            return (
                f'<li style="{self._LIST_ITEM_ICON_STYLE}">'
                f'<span style="{self._BULLET_ICON_WRAPPER_STYLE}">{bullet_html}</span>'
                f"{content_html}</li>"
            )
        else:
            return f'<li style="{self._LIST_ITEM_MARGIN}">{content_html}</li>'

    def _format_as_bullet_list(self, block: LinesBlock) -> str:
        list_items = []
        for line in block.lines:
            bullet_inline, content_inlines = self._extract_bullet_parts(line)
            list_item = self._format_bullet_list_item(bullet_inline, content_inlines)
            list_items.append(list_item)

        return self._wrap_section(
            f'<ul style="{self._UL_STYLE}">{"".join(list_items)}</ul>'
        )

    def _format_fact_list_block(self, block: FactListBlock) -> str:
        if not block.facts:
            return ""

        rows = [self._format_fact_row(fact) for fact in block.facts]
        table_html = (
            f'<table style="{self._FACT_LIST_TABLE_STYLE}">'
            f'{"".join(rows)}'
            f"</table>"
        )
        return f'<div style="{self._FACT_LIST_MARGIN}">{table_html}</div>'

    def _format_fact_row(self, fact: FactBlock) -> str:
        title_html = self._format_line_block(fact.title)
        value_html = self._format_line_block(fact.value)

        value_weight = "700" if fact.primary else "400"
        value_style = f"padding:4px 12px;font-weight:{value_weight};font-size:14px;"  # noqa: E231,E702

        return (
            "<tr>"
            f'<td style="{self._FACT_TITLE_STYLE}">{title_html}</td>'
            f'<td style="{value_style}">{value_html}</td>'
            "</tr>"
        )

    def _format_table_block(self, block: TableBlock) -> str:
        header_html = self._format_table_header(block.headers) if block.headers else ""
        body_html = self._format_table_body(block.rows)
        return f'<table style="{self._TABLE_STYLE}">{header_html}{body_html}</table>'

    def _format_table_header(self, headers: Sequence[str]) -> str:
        header_cells = "".join(
            f'<th style="{self._TABLE_HEADER_STYLE}">{escape(header)}</th>'
            for header in headers
        )
        return f"<thead><tr>{header_cells}</tr></thead>"

    def _format_table_body(self, rows: Sequence[Sequence[object]]) -> str:
        body_rows = [self._format_table_row(row) for row in rows]
        return f'<tbody>{"".join(body_rows)}</tbody>'

    def _format_table_row(self, row: Sequence[object]) -> str:
        cells = "".join(
            f'<td style="{self._TABLE_CELL_STYLE}">{escape(self._coerce_table_cell(cell))}</td>'
            for cell in row
        )
        return f"<tr>{cells}</tr>"

    def _format_expandable_block(self, block: ExpandableBlock) -> str:
        body_html = self.format_message_blocks(block.body)
        title_html = escape(block.title)
        open_attr = " open" if block.expanded else ""

        summary_html = self._build_expandable_summary(title_html)

        return (
            f'<details style="{self._EXPANDABLE_CONTAINER_STYLE}"{open_attr}>'
            f"{summary_html}"
            f'<div style="{self._EXPANDABLE_BODY_STYLE}">{body_html}</div>'
            "</details>"
        )

    def _build_expandable_summary(self, title_html: str) -> str:
        arrow = "▶"
        return (
            f'<summary style="{self._EXPANDABLE_SUMMARY_STYLE}">'
            f'<span style="{self._EXPANDABLE_ARROW_STYLE}">{arrow}</span>'
            f"{title_html}"
            "</summary>"
            f"{self._EXPANDABLE_CSS}"
        )

    def _coerce_table_cell(self, cell: object) -> str:
        if cell is None:
            return ""
        return str(cell)

    def _build_container_style(self, color: Color | None) -> str:
        styles: list[str] = list(self._CONTAINER_STYLES)
        if color and color in COLOR_MAP:
            # Replace the default border color with the status color
            styles = [
                (
                    s
                    if not s.startswith("border:")
                    else f"border:1px solid {COLOR_MAP[color]}"
                )
                for s in styles
            ]
            styles.append(f"border-left:4px solid {COLOR_MAP[color]}")  # noqa: E231
            styles.append("padding-left:28px")  # noqa: E231
        return ";".join(styles)


def format_html(message: MessageBody) -> str:
    formatter = HTMLFormatter()
    return formatter.format(message)
