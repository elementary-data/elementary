import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_type_timestamp import (
    seeds__data_type_timestamp_csv,
    models__test_type_timestamp_sql,
    models__test_type_timestamp_yml,
)


class BaseTypeTimestamp(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_type_timestamp.csv": seeds__data_type_timestamp_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_type_timestamp.yml": models__test_type_timestamp_yml,
            "test_type_timestamp.sql": models__test_type_timestamp_sql,
        }


@pytest.mark.skip(reason="TODO - implement this test")
class TestTypeTimestamp(BaseTypeTimestamp):
    pass
