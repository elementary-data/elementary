import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_hash import (
    seeds__data_hash_csv,
    models__test_hash_sql,
    models__test_hash_yml,
)


class BaseHash(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_hash.csv": seeds__data_hash_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_hash.yml": models__test_hash_yml,
            "test_hash.sql": models__test_hash_sql,
        }


class TestHash(BaseHash):
    pass
