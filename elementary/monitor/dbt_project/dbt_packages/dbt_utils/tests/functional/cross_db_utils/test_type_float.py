import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_type_float import (
    seeds__data_type_float_csv,
    models__test_type_float_sql,
    models__test_type_float_yml,
)


class BaseTypeFloat(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_type_float.csv": seeds__data_type_float_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_type_float.yml": models__test_type_float_yml,
            "test_type_float.sql": models__test_type_float_sql,
        }


@pytest.mark.skip(reason="TODO - implement this test")
class TestTypeFloat(BaseTypeFloat):
    pass
