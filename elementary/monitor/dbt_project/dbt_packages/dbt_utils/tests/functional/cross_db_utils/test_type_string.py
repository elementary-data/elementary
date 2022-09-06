import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_type_string import (
    seeds__data_type_string_csv,
    models__test_type_string_sql,
    models__test_type_string_yml,
)


class BaseTypeString(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_type_string.csv": seeds__data_type_string_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_type_string.yml": models__test_type_string_yml,
            "test_type_string.sql": models__test_type_string_sql,
        }


@pytest.mark.skip(reason="TODO - implement this test")
class TestTypeString(BaseTypeString):
    pass
