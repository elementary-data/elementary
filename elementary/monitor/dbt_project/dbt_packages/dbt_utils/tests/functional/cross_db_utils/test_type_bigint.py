import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_type_bigint import (
    seeds__data_type_bigint_csv,
    models__test_type_bigint_sql,
    models__test_type_bigint_yml,
)


class BaseTypeBigint(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_type_bigint.csv": seeds__data_type_bigint_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_type_bigint.yml": models__test_type_bigint_yml,
            "test_type_bigint.sql": models__test_type_bigint_sql,
        }


@pytest.mark.skip(reason="TODO - implement this test")
class TestTypeBigint(BaseTypeBigint):
    pass
