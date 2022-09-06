import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_right import (
    seeds__data_right_csv,
    models__test_right_sql,
    models__test_right_yml,
)


class BaseRight(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_right.csv": seeds__data_right_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_right.yml": models__test_right_yml,
            "test_right.sql": models__test_right_sql,
        }


class TestRight(BaseRight):
    pass
