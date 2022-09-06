import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_split_part import (
    seeds__data_split_part_csv,
    models__test_split_part_sql,
    models__test_split_part_yml,
)


class BaseSplitPart(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_split_part.csv": seeds__data_split_part_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_split_part.yml": models__test_split_part_yml,
            "test_split_part.sql": models__test_split_part_sql,
        }


class TestSplitPart(BaseSplitPart):
    pass
