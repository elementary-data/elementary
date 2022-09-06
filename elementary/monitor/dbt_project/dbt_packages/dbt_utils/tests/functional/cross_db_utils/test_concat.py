import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_concat import (
    seeds__data_concat_csv,
    models__test_concat_sql,
    models__test_concat_yml,
)


class BaseConcat(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_concat.csv": seeds__data_concat_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_concat.yml": models__test_concat_yml,
            "test_concat.sql": models__test_concat_sql,
        }


class TestConcat(BaseConcat):
    pass
