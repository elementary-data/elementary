import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_type_numeric import (
    seeds__data_type_numeric_csv,
    models__test_type_numeric_sql,
    models__test_type_numeric_yml,
)


class BaseTypeNumeric(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_type_numeric.csv": seeds__data_type_numeric_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_type_numeric.yml": models__test_type_numeric_yml,
            "test_type_numeric.sql": models__test_type_numeric_sql,
        }


@pytest.mark.skip(reason="TODO - implement this test")
class TestTypeNumeric(BaseTypeNumeric):
    pass
