import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_dateadd import (
    seeds__data_dateadd_csv,
    models__test_dateadd_sql,
    models__test_dateadd_yml,
)


class BaseDateAdd(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test",
            "seeds": {
                "test": {
                    "data_dateadd": {
                        "+column_types": {
                            "from_time": "timestamp",
                            "result": "timestamp",
                        },
                    },
                },
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_dateadd.csv": seeds__data_dateadd_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_dateadd.yml": models__test_dateadd_yml,
            "test_dateadd.sql": models__test_dateadd_sql,
        }


class TestDateAdd(BaseDateAdd):
    pass
