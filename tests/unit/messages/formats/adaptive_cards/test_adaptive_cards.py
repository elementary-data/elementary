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

import pytest

from elementary.messages.blocks import HeaderBlock
from elementary.messages.formats.adaptive_cards import format_adaptive_card
from elementary.messages.message_body import MessageBody
from tests.unit.messages.formats.base_test_format import BaseTestFormat
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


class TestAdaptiveCards(BaseTestFormat[dict]):
    def format(self, message_body: MessageBody) -> dict:
        return format_adaptive_card(message_body)

    def get_expected_file_path(self, name: str) -> str:
        return get_expected_json_path(FIXTURES_DIR, f"{name}.json")

    def assert_expected_value(self, result: dict, expected_file_path: Path) -> None:
        assert_expected_json(result, expected_file_path)

    @pytest.mark.parametrize(
        "version,should_raise",
        [
            pytest.param("1.6", False, id="supported_version"),
            pytest.param("1.1", True, id="unsupported_version_low"),
            pytest.param("1.7", True, id="unsupported_version_high"),
        ],
    )
    def test_format_version_validation(self, version, should_raise):
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
