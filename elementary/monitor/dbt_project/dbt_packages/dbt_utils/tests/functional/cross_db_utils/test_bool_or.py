import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_bool_or import (
    seeds__data_bool_or_csv,
    seeds__data_bool_or_expected_csv,
    models__test_bool_or_sql,
    models__test_bool_or_yml,
)


class BaseBoolOr(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_bool_or.csv": seeds__data_bool_or_csv,
            "data_bool_or_expected.csv": seeds__data_bool_or_expected_csv
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_bool_or.yml": models__test_bool_or_yml,
            "test_bool_or.sql": models__test_bool_or_sql,
        }


class TestBoolOr(BaseBoolOr):
    pass
