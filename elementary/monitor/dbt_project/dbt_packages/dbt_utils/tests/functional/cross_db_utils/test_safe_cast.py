import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_safe_cast import (
    seeds__data_safe_cast_csv,
    models__test_safe_cast_sql,
    models__test_safe_cast_yml,
)


class BaseSafeCast(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_safe_cast.csv": seeds__data_safe_cast_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_safe_cast.yml": models__test_safe_cast_yml,
            "test_safe_cast.sql": models__test_safe_cast_sql,
        }


class TestSafeCast(BaseSafeCast):
    pass
