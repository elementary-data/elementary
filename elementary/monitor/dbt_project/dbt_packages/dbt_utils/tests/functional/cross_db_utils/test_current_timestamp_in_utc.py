import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_current_timestamp_in_utc import (
    models__test_current_timestamp_in_utc_sql,
    models__test_current_timestamp_in_utc_yml,
)


class BaseCurrentTimestampInUtc(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_current_timestamp_in_utc.yml": models__test_current_timestamp_in_utc_yml,
            "test_current_timestamp_in_utc.sql": models__test_current_timestamp_in_utc_sql,
        }


class TestCurrentTimestampInUtc(BaseCurrentTimestampInUtc):
    pass
