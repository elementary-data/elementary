from pathlib import Path

from elementary.messages.formats.html import format_html
from elementary.messages.message_body import MessageBody
from tests.unit.messages.formats.base_test_format import BaseTestFormat
from tests.unit.messages.utils import assert_expected_text, get_expected_file_path

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestHTML(BaseTestFormat[str]):
    def format(self, message_body: MessageBody) -> str:
        return format_html(message_body)

    def get_expected_file_path(self, name: str) -> Path:
        return get_expected_file_path(FIXTURES_DIR, f"{name}.html")

    def assert_expected_value(self, result: str, expected_file_path: Path) -> None:
        assert_expected_text(result, expected_file_path)
