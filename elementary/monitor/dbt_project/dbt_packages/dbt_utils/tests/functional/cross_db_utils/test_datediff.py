import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_datediff import (
    seeds__data_datediff_csv,
    models__test_datediff_sql,
    models__test_datediff_yml,
)


class BaseDateDiff(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_datediff.csv": seeds__data_datediff_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_datediff.yml": models__test_datediff_yml,
            "test_datediff.sql": models__test_datediff_sql,
        }


class TestDateDiff(BaseDateDiff):
    pass
