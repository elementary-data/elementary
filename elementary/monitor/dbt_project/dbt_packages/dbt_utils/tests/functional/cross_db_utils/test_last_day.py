import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_last_day import (
    seeds__data_last_day_csv,
    models__test_last_day_sql,
    models__test_last_day_yml,
)


class BaseLastDay(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_last_day.csv": seeds__data_last_day_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_last_day.yml": models__test_last_day_yml,
            "test_last_day.sql": models__test_last_day_sql,
        }


class TestLastDay(BaseLastDay):
    pass
