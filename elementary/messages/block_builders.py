import json
from typing import List, Optional, Sequence, Tuple, Union

from .blocks import (
    CodeBlock,
    FactBlock,
    FactListBlock,
    Icon,
    IconBlock,
    InlineBlock,
    LineBlock,
    LinesBlock,
    LinkBlock,
    MentionBlock,
    TextBlock,
    TextStyle,
    WhitespaceBlock,
)

SimpleInlineBlock = Union[str, Icon]
SimpleLineBlock = Union[str, Icon, Sequence[SimpleInlineBlock]]


def _build_inline_block(
    content: SimpleInlineBlock, style: Optional[TextStyle] = None
) -> InlineBlock:
    return IconBlock(icon=content) if isinstance(content, Icon) else TextBlock(text=content, style=style)  # type: ignore


def _build_inlines(
    content: SimpleLineBlock, style: Optional[TextStyle] = None
) -> List[InlineBlock]:
    if isinstance(content, (str, Icon)):
        return [_build_inline_block(content, style)]
    return [_build_inline_block(line, style) for line in content]


def BulletListBlock(
    *,
    icon: Union[Icon, str],
    lines: List[LineBlock] = [],
    indent: int = 0,
) -> LinesBlock:
    whitespaces = [WhitespaceBlock()] * indent
    icon_inline: InlineBlock = (
        IconBlock(icon=icon) if isinstance(icon, Icon) else TextBlock(text=icon)
    )
    lines = [
        LineBlock(
            inlines=[
                *whitespaces,
                icon_inline,
                TextBlock(text=" "),
                line,
            ],
            sep="",
        )
        for line in lines
    ]
    return LinesBlock(lines=lines)


def BoldTextBlock(*, text: str) -> TextBlock:
    return TextBlock(text=text, style=TextStyle.BOLD)


def ItalicTextBlock(*, text: str) -> TextBlock:
    return TextBlock(text=text, style=TextStyle.ITALIC)


def TextLineBlock(
    *, text: SimpleLineBlock, style: Optional[TextStyle] = None
) -> LineBlock:
    return LineBlock(inlines=_build_inlines(text, style))


def BoldTextLineBlock(*, text: SimpleLineBlock) -> LineBlock:
    return TextLineBlock(text=text, style=TextStyle.BOLD)


def ItalicTextLineBlock(*, text: SimpleLineBlock) -> LineBlock:
    return TextLineBlock(text=text, style=TextStyle.ITALIC)


def LinkInlineBlocks(
    *, text: str, url: str, icon: Optional[Icon] = None
) -> list[InlineBlock]:
    inlines: list[InlineBlock] = []
    if icon:
        inlines.append(IconBlock(icon=icon))
    inlines.append(LinkBlock(text=text, url=url))
    return inlines


def LinkLineBlock(*, text: str, url: str) -> LineBlock:
    return LineBlock(inlines=LinkInlineBlocks(text=text, url=url))


def LinksLineBlock(*, links: list[tuple[str, str, Optional[Icon]]]) -> LineBlock:
    return LineBlock(
        inlines=[
            inline
            for text, url, icon in links
            for inline in LinkInlineBlocks(text=text, url=url, icon=icon)
        ]
    )


def SummaryLineBlock(
    *,
    summary: Sequence[Tuple[SimpleLineBlock, SimpleLineBlock]],
    include_empty_values: bool = False,
) -> LineBlock:
    text_blocks: List[InlineBlock] = []
    for title, value in summary:
        if not value and not include_empty_values:
            continue
        text_blocks.extend(_build_inlines(title, TextStyle.BOLD))
        text_blocks.extend(_build_inlines(value))
        text_blocks.append(TextBlock(text="|"))
    text_blocks = text_blocks[:-1]
    return LineBlock(inlines=text_blocks)


def NonPrimaryFactBlock(fact: Tuple[LineBlock, LineBlock]) -> FactBlock:
    title, value = fact
    return FactBlock(
        title=title,
        value=value,
        primary=False,
    )


def PrimaryFactBlock(fact: Tuple[LineBlock, LineBlock]) -> FactBlock:
    title, value = fact
    return FactBlock(
        title=title,
        value=value,
        primary=True,
    )


def FactsBlock(
    *,
    facts: Sequence[
        Tuple[
            SimpleLineBlock,
            SimpleLineBlock,
        ]
    ],
    include_empty_values: bool = False,
) -> FactListBlock:
    return FactListBlock(
        facts=[
            FactBlock(
                title=LineBlock(inlines=_build_inlines(title)),
                value=LineBlock(inlines=_build_inlines(value)),
            )
            for title, value in facts
            if value or include_empty_values
        ]
    )


def TitledParagraphBlock(
    *,
    title: SimpleLineBlock,
    lines: List[LineBlock],
) -> LinesBlock:
    title_line = LineBlock(inlines=_build_inlines(title, TextStyle.BOLD))
    return LinesBlock(lines=[title_line] + lines)


def JsonCodeBlock(*, content: Union[str, dict, list], indent: int = 2) -> CodeBlock:
    return CodeBlock(text=json.dumps(content, indent=indent))


def MentionLineBlock(*users: str) -> LineBlock:
    return LineBlock(inlines=[MentionBlock(user=user) for user in users], sep=", ")
