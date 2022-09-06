import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_current_timestamp import (
    models__test_current_timestamp_sql,
    models__test_current_timestamp_yml,
)


class BaseCurrentTimestamp(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_current_timestamp.yml": models__test_current_timestamp_yml,
            "test_current_timestamp.sql": models__test_current_timestamp_sql,
        }


class TestCurrentTimestamp(BaseCurrentTimestamp):
    pass
