import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_replace import (
    seeds__data_replace_csv,
    models__test_replace_sql,
    models__test_replace_yml,
)


class BaseReplace(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_replace.csv": seeds__data_replace_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_replace.yml": models__test_replace_yml,
            "test_replace.sql": models__test_replace_sql,
        }


class TestReplace(BaseReplace):
    pass
