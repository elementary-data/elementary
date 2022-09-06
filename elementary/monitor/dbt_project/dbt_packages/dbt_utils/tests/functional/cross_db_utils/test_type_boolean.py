import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_type_boolean import (
    seeds__data_type_boolean_csv,
    models__test_type_boolean_sql,
    models__test_type_boolean_yml,
)


class BaseTypeBoolean(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_type_boolean.csv": seeds__data_type_boolean_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_type_boolean.yml": models__test_type_boolean_yml,
            "test_type_boolean.sql": models__test_type_boolean_sql,
        }


@pytest.mark.skip(reason="TODO - implement this test")
class TestTypeBoolean(BaseTypeBoolean):
    pass
