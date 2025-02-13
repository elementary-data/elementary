from abc import abstractmethod
from pathlib import Path
from typing import Generic, List, TypeVar, Union

import pytest

from elementary.messages.block_builders import BulletListBlock
from elementary.messages.blocks import (
    CodeBlock,
    DividerBlock,
    ExpandableBlock,
    FactBlock,
    FactListBlock,
    HeaderBlock,
    Icon,
    IconBlock,
    LineBlock,
    LinesBlock,
    LinkBlock,
    TextBlock,
    TextStyle,
)
from elementary.messages.message_body import Color, MessageBody

T = TypeVar("T")


class BaseTestFormat(Generic[T]):
    @abstractmethod
    def format(self, message_body: MessageBody) -> T:
        raise NotImplementedError

    @abstractmethod
    def get_expected_file_path(self, name: str) -> Path:
        raise NotImplementedError

    @abstractmethod
    def assert_expected_value(self, result: T, expected_file_path: Path) -> None:
        raise NotImplementedError

    def test_format_message_body_simple_header(self):
        message_body = MessageBody(blocks=[HeaderBlock(text="Test Header")], color=None)
        expected_file_path = self.get_expected_file_path("simple_header")
        result = self.format(message_body)
        self.assert_expected_value(result, expected_file_path)

    def test_format_message_body_colored_header(self):
        message_body = MessageBody(
            blocks=[HeaderBlock(text="Test Header")], color=Color.GREEN
        )
        expected_file_path = self.get_expected_file_path("colored_header")
        result = self.format(message_body)
        self.assert_expected_value(result, expected_file_path)

    def test_format_message_body_all_icons(self):
        icon_blocks: List[Union[TextBlock, IconBlock]] = []
        for icon in Icon:
            icon_blocks.append(TextBlock(text=icon.name))
            icon_blocks.append(IconBlock(icon=icon))
        message_body = MessageBody(
            blocks=[LinesBlock(lines=[LineBlock(inlines=icon_blocks)])]
        )
        result = self.format(message_body)
        expected_file_path = self.get_expected_file_path("all_icons")
        self.assert_expected_value(result, expected_file_path)

    def test_format_message_body_text_styles(self):
        message_body = MessageBody(
            blocks=[
                LinesBlock(
                    lines=[
                        LineBlock(
                            inlines=[
                                TextBlock(text="Normal text"),
                                TextBlock(text="Bold text", style=TextStyle.BOLD),
                                TextBlock(text="Italic text", style=TextStyle.ITALIC),
                            ]
                        )
                    ]
                )
            ]
        )
        expected_file_path = self.get_expected_file_path("text_styles")
        result = self.format(message_body)
        self.assert_expected_value(result, expected_file_path)

    def test_format_message_body_fact_list(self):
        message_body = MessageBody(
            blocks=[
                FactListBlock(
                    facts=[
                        FactBlock(
                            title=LineBlock(inlines=[TextBlock(text="Status")]),
                            value=LineBlock(inlines=[TextBlock(text="Passed")]),
                        ),
                    ]
                )
            ]
        )
        expected_file_path = self.get_expected_file_path("fact_list")
        result = self.format(message_body)
        self.assert_expected_value(result, expected_file_path)

    def test_format_message_body_expandable_block(self):
        message_body = MessageBody(
            blocks=[
                ExpandableBlock(
                    title="Show More",
                    body=[
                        LinesBlock(
                            lines=[
                                LineBlock(inlines=[TextBlock(text="Hidden content")])
                            ]
                        )
                    ],
                    expanded=False,
                )
            ]
        )
        expected_file_path = self.get_expected_file_path("expandable_block")
        result = self.format(message_body)
        self.assert_expected_value(result, expected_file_path)

    def test_format_message_body_divider_blocks(self):
        message_body = MessageBody(
            blocks=[
                HeaderBlock(text="First Section"),
                DividerBlock(),
                HeaderBlock(text="Second Section"),
            ]
        )
        expected_file_path = self.get_expected_file_path("divider_blocks")
        result = self.format(message_body)
        self.assert_expected_value(result, expected_file_path)

    def test_format_message_body_bullet_lists(self):
        message_body = MessageBody(
            blocks=[
                BulletListBlock(
                    icon="-",
                    lines=[
                        LineBlock(inlines=[TextBlock(text="First bullet")]),
                        LineBlock(inlines=[TextBlock(text="Second bullet")]),
                    ],
                ),
                BulletListBlock(
                    icon=Icon.CHECK,
                    lines=[
                        LineBlock(inlines=[TextBlock(text="Check item 1")]),
                        LineBlock(inlines=[TextBlock(text="Check item 2")]),
                    ],
                ),
            ]
        )
        expected_file_path = self.get_expected_file_path("bullet_list")
        result = self.format(message_body)
        self.assert_expected_value(result, expected_file_path)

    def test_format_message_body_nested_expandable(self):
        message_body = MessageBody(
            blocks=[
                ExpandableBlock(
                    title="Outer Block",
                    body=[
                        LinesBlock(
                            lines=[
                                LineBlock(
                                    inlines=[
                                        IconBlock(icon=Icon.MAGNIFYING_GLASS),
                                        TextBlock(
                                            text="Title with Icon", style=TextStyle.BOLD
                                        ),
                                    ]
                                ),
                                LineBlock(
                                    inlines=[
                                        TextBlock(text="Some content with a"),
                                        LinkBlock(
                                            text="link", url="https://example.com"
                                        ),
                                    ]
                                ),
                            ]
                        ),
                        ExpandableBlock(
                            title="Inner Block",
                            body=[
                                LinesBlock(
                                    lines=[
                                        LineBlock(
                                            inlines=[TextBlock(text="Inner content")]
                                        )
                                    ]
                                )
                            ],
                            expanded=True,
                        ),
                    ],
                    expanded=False,
                )
            ]
        )
        expected_file_path = self.get_expected_file_path("nested_expandable")
        result = self.format(message_body)
        self.assert_expected_value(result, expected_file_path)

    @pytest.mark.parametrize(
        "color,expected_file",
        [
            pytest.param(None, "all_blocks_no_color", id="no_color"),
            pytest.param(Color.RED, "all_blocks_red", id="red"),
            pytest.param(Color.YELLOW, "all_blocks_yellow", id="yellow"),
            pytest.param(Color.GREEN, "all_blocks_green", id="green"),
        ],
    )
    def test_format_message_body_all_blocks(self, color, expected_file):
        """Test a comprehensive message that includes all block types with different colors."""
        message_body = MessageBody(
            blocks=[
                HeaderBlock(text="Main Header"),
                LinesBlock(
                    lines=[
                        LineBlock(
                            inlines=[
                                TextBlock(text="Normal text"),
                                TextBlock(text="Bold text", style=TextStyle.BOLD),
                                TextBlock(text="Italic text", style=TextStyle.ITALIC),
                            ]
                        )
                    ]
                ),
                BulletListBlock(
                    icon="-",
                    lines=[
                        LineBlock(inlines=[TextBlock(text="First bullet point")]),
                        LineBlock(inlines=[TextBlock(text="Second bullet point")]),
                    ],
                ),
                BulletListBlock(
                    icon=Icon.CHECK,
                    lines=[LineBlock(inlines=[TextBlock(text="Check item")])],
                ),
                FactListBlock(
                    facts=[
                        FactBlock(
                            title=LineBlock(inlines=[TextBlock(text="Status")]),
                            value=LineBlock(inlines=[TextBlock(text="Passed")]),
                        ),
                        FactBlock(
                            title=LineBlock(inlines=[TextBlock(text="Tags")]),
                            value=LineBlock(inlines=[TextBlock(text="test, example")]),
                        ),
                    ]
                ),
                ExpandableBlock(
                    title="Show Details",
                    body=[
                        LinesBlock(
                            lines=[
                                LineBlock(
                                    inlines=[
                                        IconBlock(icon=Icon.MAGNIFYING_GLASS),
                                        TextBlock(
                                            text="Details Section", style=TextStyle.BOLD
                                        ),
                                    ]
                                ),
                                LineBlock(
                                    inlines=[
                                        TextBlock(text="Here's some content with a"),
                                        LinkBlock(
                                            text="link", url="https://example.com"
                                        ),
                                    ]
                                ),
                            ]
                        )
                    ],
                    expanded=False,
                ),
            ],
            color=color,
        )
        expected_file_path = self.get_expected_file_path(expected_file)
        result = self.format(message_body)
        self.assert_expected_value(result, expected_file_path)

    @pytest.mark.parametrize(
        "text_length",
        [
            pytest.param(50, id="short_code"),
            pytest.param(200, id="medium_code"),
            pytest.param(500, id="long_code"),
        ],
    )
    def test_format_message_body_code_block(self, text_length: int):
        lorem_text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " * (
            (text_length + 49) // 50
        )
        lorem_text = lorem_text[:text_length]

        message_body = MessageBody(blocks=[CodeBlock(text=lorem_text)])
        expected_file_path = self.get_expected_file_path(f"code_block_{text_length}")
        result = self.format(message_body)
        self.assert_expected_value(result, expected_file_path)

    @pytest.mark.parametrize(
        "text_length",
        [
            pytest.param(50, id="short_text"),
            pytest.param(200, id="medium_text"),
            pytest.param(500, id="long_text"),
            pytest.param(1000, id="very_long_text"),
        ],
    )
    def test_format_message_body_text_length(self, text_length: int):
        lorem_text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " * (
            (text_length + 49) // 50
        )
        lorem_text = lorem_text[:text_length]

        message_body = MessageBody(
            blocks=[LinesBlock(lines=[LineBlock(inlines=[TextBlock(text=lorem_text)])])]
        )
        expected_file_path = self.get_expected_file_path(f"text_length_{text_length}")
        result = self.format(message_body)
        self.assert_expected_value(result, expected_file_path)
