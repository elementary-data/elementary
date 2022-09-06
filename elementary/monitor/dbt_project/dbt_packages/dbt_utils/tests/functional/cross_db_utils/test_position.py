import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_position import (
    seeds__data_position_csv,
    models__test_position_sql,
    models__test_position_yml,
)


class BasePosition(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_position.csv": seeds__data_position_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_position.yml": models__test_position_yml,
            "test_position.sql": models__test_position_sql,
        }


class TestPosition(BasePosition):
    pass
