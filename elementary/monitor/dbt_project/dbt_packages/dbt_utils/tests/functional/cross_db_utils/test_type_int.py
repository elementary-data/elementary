import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_type_int import (
    seeds__data_type_int_csv,
    models__test_type_int_sql,
    models__test_type_int_yml,
)


class BaseTypeInt(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_type_int.csv": seeds__data_type_int_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_type_int.yml": models__test_type_int_yml,
            "test_type_int.sql": models__test_type_int_sql,
        }


@pytest.mark.skip(reason="TODO - implement this test")
class TestTypeInt(BaseTypeInt):
    pass
