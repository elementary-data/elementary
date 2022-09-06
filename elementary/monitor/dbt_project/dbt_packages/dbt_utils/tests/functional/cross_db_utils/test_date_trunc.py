import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_date_trunc import (
    seeds__data_date_trunc_csv,
    models__test_date_trunc_sql,
    models__test_date_trunc_yml,
)


class BaseDateTrunc(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_date_trunc.csv": seeds__data_date_trunc_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_trunc.yml": models__test_date_trunc_yml,
            "test_date_trunc.sql": models__test_date_trunc_sql,
        }


class TestDateTrunc(BaseDateTrunc):
    pass
