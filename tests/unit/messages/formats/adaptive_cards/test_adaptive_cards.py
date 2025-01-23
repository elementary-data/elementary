"""
Tests for adaptive cards message format.

The expected results are saved as JSON files in the resources/ directory.
These files can be viewed and tested using:
1. VS Code's Adaptive Cards extension
2. Adaptive Cards Designer (https://adaptivecards.io/designer/) - useful for checking how cards
   look across different products, devices, themes and schema versions
"""

import uuid
from pathlib import Path
from typing import List, Union

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
from elementary.messages.formats.adaptive_cards import format_adaptive_card
from elementary.messages.message_body import Color, MessageBody
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture(autouse=True)
def mock_uuid(monkeypatch):
    class MockUUID:
        def __init__(self):
            self.counter = 0

        def __call__(self):
            self.counter += 1
            return uuid.UUID(
                f"00000000-0000-0000-0000-{self.counter:012d}"  # noqa: E231
            )

    mock = MockUUID()
    monkeypatch.setattr(uuid, "uuid4", mock)
    return mock


def test_format_message_body_simple_header():
    message_body = MessageBody(blocks=[HeaderBlock(text="Test Header")], color=None)
    expected_json_path = get_expected_json_path(FIXTURES_DIR, "simple_header.json")
    result = format_adaptive_card(message_body)
    assert_expected_json(result, expected_json_path)


def test_format_message_body_colored_header():
    message_body = MessageBody(
        blocks=[HeaderBlock(text="Test Header")], color=Color.GREEN
    )
    expected_json_path = get_expected_json_path(FIXTURES_DIR, "colored_header.json")
    result = format_adaptive_card(message_body)
    assert_expected_json(result, expected_json_path)


def test_format_message_body_all_icons():
    icon_blocks: List[Union[TextBlock, IconBlock]] = []
    for icon in Icon:
        icon_blocks.append(TextBlock(text=icon.name))
        icon_blocks.append(IconBlock(icon=icon))
    message_body = MessageBody(
        blocks=[LinesBlock(lines=[LineBlock(inlines=icon_blocks)])]
    )
    result = format_adaptive_card(message_body)
    expected_json_path = get_expected_json_path(FIXTURES_DIR, "all_icons.json")
    assert_expected_json(result, expected_json_path)


def test_format_message_body_text_styles():
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
    expected_json_path = get_expected_json_path(FIXTURES_DIR, "text_styles.json")
    result = format_adaptive_card(message_body)
    assert_expected_json(result, expected_json_path)


def test_format_message_body_fact_list():
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
    expected_json_path = get_expected_json_path(FIXTURES_DIR, "fact_list.json")
    result = format_adaptive_card(message_body)
    assert_expected_json(result, expected_json_path)


def test_format_message_body_expandable_block():
    message_body = MessageBody(
        blocks=[
            ExpandableBlock(
                title="Show More",
                body=[
                    LinesBlock(
                        lines=[LineBlock(inlines=[TextBlock(text="Hidden content")])]
                    )
                ],
                expanded=False,
            )
        ]
    )
    expected_json_path = get_expected_json_path(FIXTURES_DIR, "expandable_block.json")
    result = format_adaptive_card(message_body)
    assert_expected_json(result, expected_json_path)


def test_format_message_body_divider_blocks():
    message_body = MessageBody(
        blocks=[
            HeaderBlock(text="First Section"),
            DividerBlock(),
            HeaderBlock(text="Second Section"),
        ]
    )
    expected_json_path = get_expected_json_path(FIXTURES_DIR, "divider_blocks.json")
    result = format_adaptive_card(message_body)
    assert_expected_json(result, expected_json_path)


def test_format_message_body_bullet_lists():
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
    expected_json_path = get_expected_json_path(FIXTURES_DIR, "bullet_list.json")
    result = format_adaptive_card(message_body)
    assert_expected_json(result, expected_json_path)


def test_format_message_body_nested_expandable():
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
                                    LinkBlock(text="link", url="https://example.com"),
                                ]
                            ),
                        ]
                    ),
                    ExpandableBlock(
                        title="Inner Block",
                        body=[
                            LinesBlock(
                                lines=[
                                    LineBlock(inlines=[TextBlock(text="Inner content")])
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
    expected_json_path = get_expected_json_path(FIXTURES_DIR, "nested_expandable.json")
    result = format_adaptive_card(message_body)
    assert_expected_json(result, expected_json_path)


@pytest.mark.parametrize(
    "color,expected_file",
    [
        pytest.param(None, "all_blocks_no_color.json", id="no_color"),
        pytest.param(Color.RED, "all_blocks_red.json", id="red"),
        pytest.param(Color.YELLOW, "all_blocks_yellow.json", id="yellow"),
        pytest.param(Color.GREEN, "all_blocks_green.json", id="green"),
    ],
)
def test_format_message_body_all_blocks(color, expected_file):
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
                                    LinkBlock(text="link", url="https://example.com"),
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
    expected_json_path = get_expected_json_path(FIXTURES_DIR, expected_file)
    result = format_adaptive_card(message_body)
    assert_expected_json(result, expected_json_path)


@pytest.mark.parametrize(
    "version,should_raise",
    [
        pytest.param("1.6", False, id="supported_version"),
        pytest.param("1.1", True, id="unsupported_version_low"),
        pytest.param("1.7", True, id="unsupported_version_high"),
    ],
)
def test_format_version_validation(version, should_raise):
    message_body = MessageBody(blocks=[HeaderBlock(text="Test")])

    if should_raise:
        try:
            format_adaptive_card(message_body, version=version)
            assert False, f"Expected ValueError for version {version}"
        except ValueError:
            pass
    else:
        result = format_adaptive_card(message_body, version=version)
        assert result["version"] == version
        assert result["type"] == "AdaptiveCard"


@pytest.mark.parametrize(
    "text_length",
    [
        pytest.param(50, id="short_code"),
        pytest.param(200, id="medium_code"),
        pytest.param(500, id="long_code"),
    ],
)
def test_format_message_body_code_block(text_length: int):
    lorem_text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " * (
        (text_length + 49) // 50
    )
    lorem_text = lorem_text[:text_length]

    message_body = MessageBody(blocks=[CodeBlock(text=lorem_text)])
    expected_json_path = get_expected_json_path(
        FIXTURES_DIR, f"code_block_{text_length}.json"
    )
    result = format_adaptive_card(message_body)
    assert_expected_json(result, expected_json_path)


@pytest.mark.parametrize(
    "text_length",
    [
        pytest.param(50, id="short_text"),
        pytest.param(200, id="medium_text"),
        pytest.param(500, id="long_text"),
        pytest.param(1000, id="very_long_text"),
    ],
)
def test_format_message_body_text_length(text_length: int):
    lorem_text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " * (
        (text_length + 49) // 50
    )
    lorem_text = lorem_text[:text_length]

    message_body = MessageBody(
        blocks=[LinesBlock(lines=[LineBlock(inlines=[TextBlock(text=lorem_text)])])]
    )
    expected_json_path = get_expected_json_path(
        FIXTURES_DIR, f"text_length_{text_length}.json"
    )
    result = format_adaptive_card(message_body)
    assert_expected_json(result, expected_json_path)
