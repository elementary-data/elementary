from pathlib import Path

from elementary.messages.formats.block_kit import format_block_kit
from elementary.messages.message_body import MessageBody
from tests.unit.messages.formats.base_test_format import BaseTestFormat
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestBlockKit(BaseTestFormat[dict]):
    def format(self, message_body: MessageBody) -> dict:
        return format_block_kit(message_body)

    def get_expected_file_path(self, name: str) -> str:
        return get_expected_json_path(FIXTURES_DIR, f"{name}.json")

    def assert_expected_value(self, result: dict, expected_file_path: Path) -> None:
        assert_expected_json(result, expected_file_path)
