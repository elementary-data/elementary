import pytest

from elementary.monitor.fetchers.tests.schema import TestResultDBRowSchema


@pytest.fixture
def base_test_result():
    """Base test result data for TestResultDBRowSchema tests."""
    return {
        "id": "test_id_1",
        "test_unique_id": "unique_id_1",
        "elementary_unique_id": "elem_id_1",
        "detected_at": "2023-01-01 10:00:00",
        "schema_name": "test_schema",
        "column_name": None,
        "test_type": "dbt_test",
        "test_sub_type": "generic",
        "test_results_description": "Test description",
        "original_path": "path/to/test",
        "other": None,
        "test_name": "test_name",
        "test_params": {},
        "status": "pass",
        "days_diff": 1,
        "invocations_rank_index": 1,
        "meta": {},
        "model_meta": {},
    }


class TestTestResultDBRowSchema:
    """Tests for TestResultDBRowSchema validation."""

    def test_severity_error(self, base_test_result):
        """Test that 'ERROR' severity is handled correctly."""
        data = {**base_test_result, "severity": "ERROR"}
        result = TestResultDBRowSchema(**data)
        assert result.severity == "ERROR"

    def test_severity_warn(self, base_test_result):
        """Test that 'WARN' severity is handled correctly."""
        data = {**base_test_result, "severity": "WARN"}
        result = TestResultDBRowSchema(**data)
        assert result.severity == "WARN"

    def test_severity_none_string_lowercase(self, base_test_result):
        """Test that lowercase 'none' string is normalized to None.
        
        This addresses issue #2084 where dbt-fusion returns "none" as a string
        instead of null/None, causing validation errors.
        """
        data = {**base_test_result, "severity": "none"}
        result = TestResultDBRowSchema(**data)
        assert result.severity is None

    def test_severity_none_string_uppercase(self, base_test_result):
        """Test that uppercase 'NONE' string is normalized to None."""
        data = {**base_test_result, "severity": "NONE"}
        result = TestResultDBRowSchema(**data)
        assert result.severity is None

    def test_severity_none_string_mixed_case(self, base_test_result):
        """Test that mixed case 'None' string is normalized to None."""
        data = {**base_test_result, "severity": "None"}
        result = TestResultDBRowSchema(**data)
        assert result.severity is None

    def test_severity_python_none(self, base_test_result):
        """Test that Python None is handled correctly."""
        data = {**base_test_result, "severity": None}
        result = TestResultDBRowSchema(**data)
        assert result.severity is None

    def test_severity_missing_field(self, base_test_result):
        """Test that missing severity field defaults to None."""
        # Remove severity from base_test_result to simulate it not being provided
        data = {k: v for k, v in base_test_result.items() if k != "severity"}
        result = TestResultDBRowSchema(**data)
        assert result.severity is None

    def test_severity_empty_string(self, base_test_result):
        """Test that empty string severity is preserved (not normalized to None)."""
        data = {**base_test_result, "severity": ""}
        result = TestResultDBRowSchema(**data)
        # Empty string should be preserved as-is
        assert result.severity == ""
