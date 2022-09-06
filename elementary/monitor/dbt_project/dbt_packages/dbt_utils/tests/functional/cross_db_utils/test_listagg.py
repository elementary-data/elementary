import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_listagg import (
    seeds__data_listagg_csv,
    seeds__data_listagg_output_csv,
    models__test_listagg_sql,
    models__test_listagg_yml,
)


class BaseListagg(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_listagg.csv": seeds__data_listagg_csv,
            "data_listagg_output.csv": seeds__data_listagg_output_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yml": models__test_listagg_yml,
            "test_listagg.sql": models__test_listagg_sql,
        }


class TestListagg(BaseListagg):
    pass
