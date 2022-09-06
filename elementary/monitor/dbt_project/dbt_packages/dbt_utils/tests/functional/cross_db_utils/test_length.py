import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_length import (
    seeds__data_length_csv,
    models__test_length_sql,
    models__test_length_yml,
)


class BaseLength(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_length.csv": seeds__data_length_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_length.yml": models__test_length_yml,
            "test_length.sql": models__test_length_sql,
        }


class TestLength(BaseLength):
    pass
