from __future__ import annotations

from html import escape
from typing import Sequence

from elementary.messages.blocks import (
    ActionBlock,
    ActionsBlock,
    CodeBlock,
    DividerBlock,
    DropdownActionBlock,
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
    UserSelectActionBlock,
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
    _CONTAINER_STYLES = [
        "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif",
        "font-size:14px",
        "line-height:1.5",
        "color:#1f2937",
        "background-color:#ffffff",
        "border:1px solid #e5e7eb",
        "border-radius:6px",
        "padding:16px",
    ]

    _SECTION_MARGIN = "margin:0 0 12px"

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
            return self._wrap_section(
                f'<h1 style="margin:0;font-size:18px;line-height:1.4;">{escape(block.text)}</h1>'
            )
        elif isinstance(block, CodeBlock):
            code_html = escape(block.text)
            return self._wrap_section(
                "<pre style=\"margin:0;padding:12px;"
                "background-color:#f8fafc;border-radius:4px;"
                "font-family:'SFMono-Regular',Consolas,'Liberation Mono',Menlo,monospace;"
                "font-size:13px;line-height:1.5;white-space:pre-wrap;\">"
                f"{code_html}</pre>"
            )
        elif isinstance(block, LinesBlock):
            return self._format_lines_block(block)
        elif isinstance(block, FactListBlock):
            return self._format_fact_list_block(block)
        elif isinstance(block, TableBlock):
            return self._format_table_block(block)
        elif isinstance(block, ExpandableBlock):
            return self._format_expandable_block(block)
        elif isinstance(block, DividerBlock):
            return '<hr style="border:none;border-top:1px solid #e5e7eb;margin:16px 0;" />'
        elif isinstance(block, ActionsBlock):
            return self._format_actions_block(block)
        else:
            raise ValueError(f"Unsupported message block type: {type(block)}")

    def _wrap_section(self, html: str) -> str:
        return f'<div style="{self._SECTION_MARGIN}">{html}</div>'

    def _format_icon(self, icon: Icon) -> str:
        return f'<span style="margin-right:4px;">{escape(ICON_TO_UNICODE[icon])}</span>'

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
            url = escape(block.url, quote=True)
            text = escape(block.text)
            return (
                '<a style="color:#2563eb;text-decoration:none;" '
                f'href="{url}" target="_blank" rel="noopener noreferrer">{text}</a>'
            )
        elif isinstance(block, InlineCodeBlock):
            return (
                "<code style=\"font-family:'SFMono-Regular',Consolas,'Liberation Mono',Menlo,monospace;"
                "background-color:#eef2ff;border-radius:3px;padding:1px 4px;font-size:12px;\">"
                f"{escape(block.code)}</code>"
            )
        elif isinstance(block, MentionBlock):
            return f'<span style="color:#0ea5e9;">{escape(block.user)}</span>'
        elif isinstance(block, LineBlock):
            return self._format_line_block(block)
        elif isinstance(block, WhitespaceBlock):
            return "&nbsp;"
        else:
            raise ValueError(f"Unsupported inline block type: {type(block)}")

    def _format_line_block(self, block: LineBlock) -> str:
        inlines = [self._format_inline_block(inline) for inline in block.inlines]
        separator = escape(block.sep, quote=False)
        return separator.join(inlines)

    def _format_lines_block(self, block: LinesBlock) -> str:
        lines_html = [
            f'<div style="margin:0;">{self._format_line_block(line_block)}</div>'
            for line_block in block.lines
        ]
        if not lines_html:
            return ""
        return self._wrap_section("".join(lines_html))

    def _format_fact_list_block(self, block: FactListBlock) -> str:
        if not block.facts:
            return ""
        rows = [self._format_fact_row(fact) for fact in block.facts]
        table_html = (
            '<table style="width:100%;border-collapse:separate;border-spacing:0 6px;">'
            + "".join(rows)
            + "</table>"
        )
        return self._wrap_section(table_html)

    def _format_fact_row(self, fact: FactBlock) -> str:
        title_html = self._format_line_block(fact.title)
        value_html = self._format_line_block(fact.value)
        title_style = (
            "padding:4px 12px;font-weight:600;color:#111827;"
            "background-color:#f3f4f6;border-radius:4px 0 0 4px;"
            "white-space:nowrap;"
        )
        value_weight = "700" if fact.primary else "400"
        value_style = (
            "padding:4px 12px;border:1px solid #f3f4f6;border-left:none;"
            "border-radius:0 4px 4px 0;font-weight:{weight};"
        ).format(weight=value_weight)
        return (
            "<tr>"
            f'<td style="{title_style}">{title_html}</td>'
            f'<td style="{value_style}">{value_html}</td>'
            "</tr>"
        )

    def _format_table_block(self, block: TableBlock) -> str:
        header_html = ""
        if block.headers:
            header_cells = "".join(
                f'<th style="text-align:left;padding:8px;border-bottom:1px solid #e5e7eb;'
                f'font-weight:600;background-color:#f8fafc;">{escape(header)}</th>'
                for header in block.headers
            )
            header_html = f"<thead><tr>{header_cells}</tr></thead>"
        body_rows = [
            "<tr>"
            + "".join(
                f'<td style="padding:8px;border-bottom:1px solid #f3f4f6;vertical-align:top;">'
                f"{escape(self._coerce_table_cell(cell))}</td>"
                for cell in row
            )
            + "</tr>"
            for row in block.rows
        ]
        body_html = "<tbody>" + "".join(body_rows) + "</tbody>"
        table_style = (
            "width:100%;border-collapse:collapse;margin:0 0 12px;"
            "border:1px solid #e5e7eb;border-radius:6px;overflow:hidden;"
        )
        return f'<table style="{table_style}">{header_html}{body_html}</table>'

    def _format_expandable_block(self, block: ExpandableBlock) -> str:
        body_html = self.format_message_blocks(block.body)
        title_html = escape(block.title)
        container_style = (
            "border:1px solid #e5e7eb;border-radius:6px;margin:16px 0;overflow:hidden;"
        )
        title_style = (
            "margin:0;padding:12px 16px;font-weight:600;background-color:#f8fafc;"
        )
        body_style = "padding:12px 16px;"
        return (
            f'<div style="{container_style}">'
            f'<div style="{title_style}">{title_html}</div>'
            f'<div style="{body_style}">{body_html}</div>'
            "</div>"
        )

    def _format_actions_block(self, block: ActionsBlock) -> str:
        if not block.actions:
            return ""
        rendered_actions = [self._format_action_item(action) for action in block.actions]
        actions_html = "".join(rendered_actions)
        return self._wrap_section(
            f'<div style="display:flex;flex-wrap:wrap;gap:8px;">{actions_html}</div>'
        )

    def _format_action_item(self, block: ActionBlock) -> str:
        if isinstance(block, DropdownActionBlock):
            options = ", ".join(escape(option.text) for option in block.options)
            placeholder = escape(block.placeholder or "Select an option")
            return (
                '<div style="padding:8px 12px;border:1px solid #d1d5db;border-radius:4px;'
                f'background-color:#f9fafb;">{placeholder}: {options}</div>'
            )
        elif isinstance(block, UserSelectActionBlock):
            placeholder = escape(block.placeholder or "Assign user")
            return (
                '<div style="padding:8px 12px;border:1px solid #d1d5db;border-radius:4px;'
                f'background-color:#f9fafb;">{placeholder}</div>'
            )
        else:
            raise ValueError(f"Unsupported action block type: {type(block)}")

    def _coerce_table_cell(self, cell: object) -> str:
        if cell is None:
            return ""
        return str(cell)

    def _build_container_style(self, color: Color | None) -> str:
        styles: list[str] = list(self._CONTAINER_STYLES)
        if color and color in COLOR_MAP:
            styles.append(f"border-left:4px solid {COLOR_MAP[color]}")
            styles.append("padding-left:12px")
        return ";".join(styles)


def format_html(message: MessageBody) -> str:
    formatter = HTMLFormatter()
    return formatter.format(message)
