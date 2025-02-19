import json
import os
from pathlib import Path

from deepdiff import DeepDiff

from elementary.utils.log import get_logger

logger = get_logger(__name__)


# Set to True to override the expected JSON files with the actual results
# This is useful for updating the expected JSON files with the actual results, for development purposes only!
OVERRIDE = os.getenv("OVERRIDE", "false").lower() == "true"


def get_expected_json_path(fixture_dir: Path, filename: str) -> Path:
    path = fixture_dir / filename
    if not path.exists():
        path.write_text(json.dumps({}))
    return path


def assert_expected_json(result: dict, expected_json_path: Path) -> None:
    expected = json.loads(expected_json_path.read_text())
    if OVERRIDE:
        logger.warning(f"Overriding expected JSON file: {expected_json_path}")
        expected_json_path.write_text(json.dumps(result, indent=2) + "\n")
    else:
        try:
            assert result == expected
        except AssertionError as e:
            diff = DeepDiff(expected, result)
            error_message = (
                f"\nExpected JSON: \n{json.dumps(expected, indent=2)}\n"
                f"\nActual JSON: \n{json.dumps(result, indent=2)}\n"
                f"\nDiff: \n{diff.to_json(indent=2)}\n"
            )
            raise AssertionError(error_message) from e
