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
                f'<h1 style="margin:0;font-size:18px;line-height:1.4;">{escape(block.text)}</h1>'  # noqa: E231,E702
            )
        elif isinstance(block, CodeBlock):
            code_html = escape(block.text)
            return self._wrap_section(
                '<pre style="margin:0;padding:12px;'
                "background-color:#f8fafc;border-radius:4px;"
                "font-family:'SFMono-Regular',Consolas,'Liberation Mono',Menlo,monospace;"
                "font-size:13px;line-height:1.5;white-space:pre-wrap;"
                'max-height:400px;overflow-y:auto;">'
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
            return (
                '<hr style="border:none;border-top:1px solid #e5e7eb;margin:16px 0;" />'
            )
        elif isinstance(block, ActionsBlock):
            # Not supported in HTML emails (no interactivity without JavaScript)
            return ""
        else:
            raise ValueError(f"Unsupported message block type: {type(block)}")

    def _wrap_section(self, html: str) -> str:
        return f'<div style="{self._SECTION_MARGIN}">{html}</div>'

    def _format_icon(self, icon: Icon) -> str:
        return f'<span style="margin-right:4px;">{escape(ICON_TO_UNICODE[icon])}</span>'  # noqa: E231,E702

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
                'background-color:#eef2ff;border-radius:3px;padding:1px 4px;font-size:12px;">'
                f"{escape(block.code)}</code>"
            )
        elif isinstance(block, MentionBlock):
            return f'<span style="color:#0ea5e9;">{escape(block.user)}</span>'  # noqa: E231,E702
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
        if not block.lines:
            return ""

        # Check if this is a bullet list (all lines start with icon/bullet + space)
        is_bullet_list = self._is_bullet_list(block)

        if is_bullet_list:
            return self._format_as_bullet_list(block)

        lines_html = [
            f'<div style="margin:0;">{self._format_line_block(line_block)}</div>'  # noqa: E231,E702
            for line_block in block.lines
        ]
        return self._wrap_section("".join(lines_html))

    def _is_bullet_list(self, block: LinesBlock) -> bool:
        """Check if a LinesBlock is a bullet list pattern."""
        if not block.lines:
            return False

        for line in block.lines:
            if len(line.inlines) < 3:
                return False
            # Check pattern: [optional whitespaces...] + (icon or bullet text) + space + content
            # Skip leading whitespaces
            idx = 0
            while idx < len(line.inlines) and isinstance(
                line.inlines[idx], WhitespaceBlock
            ):
                idx += 1

            if idx >= len(line.inlines):
                return False

            # Next should be IconBlock or short TextBlock (bullet marker)
            bullet = line.inlines[idx]
            if isinstance(bullet, IconBlock):
                continue
            elif isinstance(bullet, TextBlock) and len(bullet.text) <= 2:
                continue
            else:
                return False

        return True

    def _format_as_bullet_list(self, block: LinesBlock) -> str:
        """Format a LinesBlock as HTML <ul> list."""
        list_items = []
        for line in block.lines:
            # Extract bullet marker and content
            idx = 0
            # Skip leading whitespaces
            while idx < len(line.inlines) and isinstance(
                line.inlines[idx], WhitespaceBlock
            ):
                idx += 1

            # Get the bullet icon/text
            bullet_inline = line.inlines[idx]
            idx += 1

            # Skip the space after bullet
            if idx < len(line.inlines):
                space_inline = line.inlines[idx]
                if isinstance(space_inline, TextBlock) and space_inline.text == " ":
                    idx += 1

            # Rest is the content
            content_inlines = line.inlines[idx:]
            content_html = "".join(
                [self._format_inline_block(inline) for inline in content_inlines]
            )

            # Format the bullet marker
            if isinstance(bullet_inline, IconBlock):
                bullet_html = self._format_icon(bullet_inline.icon)
                list_items.append(
                    f'<li style="margin:0 0 4px;list-style:none;">'  # noqa: E231,E702
                    f'<span style="margin-right:6px;">{bullet_html}</span>{content_html}</li>'  # noqa: E231,E702
                )
            else:
                # Text bullet - use native list styling
                list_items.append(
                    f'<li style="margin:0 0 4px;">{content_html}</li>'  # noqa: E231,E702
                )

        ul_style = (
            "margin:0;padding-left:24px;list-style-position:outside;"  # noqa: E231,E702
        )
        return self._wrap_section(f'<ul style="{ul_style}">{"".join(list_items)}</ul>')

    def _format_fact_list_block(self, block: FactListBlock) -> str:
        if not block.facts:
            return ""
        rows = [self._format_fact_row(fact) for fact in block.facts]
        table_html = (
            '<table style="width:100%;border-collapse:collapse;">'
            + "".join(rows)
            + "</table>"
        )
        # Use custom margin with +8px on top and bottom
        return f'<div style="margin:8px 0 20px">{table_html}</div>'  # noqa: E231

    def _format_fact_row(self, fact: FactBlock) -> str:
        title_html = self._format_line_block(fact.title)
        value_html = self._format_line_block(fact.value)
        title_style = (  # noqa: E231,E702
            "padding:4px 12px;font-weight:600;font-size:14px;color:#111827;"
            "width:160px;white-space:nowrap;"
        )
        value_weight = "700" if fact.primary else "400"
        value_style = f"padding:4px 12px;font-weight:{value_weight};font-size:14px;"  # noqa: E231,E702
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
                f'<th style="text-align:left;padding:8px;border-bottom:1px solid #e5e7eb;'  # noqa: E231,E702
                f'font-weight:600;font-size:14px;background-color:#f8fafc;">{escape(header)}</th>'  # noqa: E231,E702
                for header in block.headers
            )
            header_html = f"<thead><tr>{header_cells}</tr></thead>"
        body_rows = [
            "<tr>"
            + "".join(
                f'<td style="padding:8px;border-bottom:1px solid #f3f4f6;vertical-align:top;font-size:14px;">'  # noqa: E231,E702
                f"{escape(self._coerce_table_cell(cell))}</td>"
                for cell in row
            )
            + "</tr>"
            for row in block.rows
        ]
        body_html = "<tbody>" + "".join(body_rows) + "</tbody>"
        table_style = (  # noqa: E231,E702
            "width:100%;border-collapse:collapse;margin:0 0 12px;"
            "border:1px solid #e5e7eb;border-radius:6px;overflow:hidden;"
        )
        return f'<table style="{table_style}">{header_html}{body_html}</table>'

    def _format_expandable_block(self, block: ExpandableBlock) -> str:
        body_html = self.format_message_blocks(block.body)
        title_html = escape(block.title)
        container_style = "border:1px solid #e5e7eb;border-radius:6px;margin:16px 0;"
        # Hide native disclosure triangle with list-style and webkit-details-marker
        summary_style = (
            "padding:12px 16px;font-weight:600;background-color:#f8fafc;"
            "cursor:pointer;font-size:14px;user-select:none;-webkit-user-select:none;"
            "list-style:none;"
        )
        # Always use right arrow - CSS will rotate it when opened
        arrow = "▶"
        arrow_style = (
            "display:inline-block;margin-right:8px;color:#6b7280;font-size:10px;"
            "transition:transform 0.2s ease;"  # noqa: E231,E702
        )
        body_style = (
            "padding:12px 16px;border-top:1px solid #e5e7eb;"  # noqa: E231,E702
        )
        open_attr = " open" if block.expanded else ""
        # Add CSS to rotate arrow when details is open and hide webkit disclosure marker
        css_style = (
            "<style>"
            "summary::-webkit-details-marker{display:none;}"
            "details[open] > summary > span{transform:rotate(90deg);}"
            "</style>"
        )
        summary_with_marker_hidden = (
            f'<summary style="{summary_style}">'
            f'<span style="{arrow_style}">{arrow}</span>'
            f"{title_html}"
            "</summary>"
            f"{css_style}"
        )
        return (
            f'<details style="{container_style}"{open_attr}>'
            f"{summary_with_marker_hidden}"
            f'<div style="{body_style}">{body_html}</div>'
            "</details>"
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
