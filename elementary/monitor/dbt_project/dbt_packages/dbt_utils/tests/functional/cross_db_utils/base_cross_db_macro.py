import os
import pytest
from dbt.tests.util import run_dbt
from tests.functional.cross_db_utils.fixture_cross_db_macro import (
    macros__test_assert_equal_sql,
)


class BaseCrossDbMacro:
    # install this repo as a package!
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": os.getcwd()}]}

    # setup
    @pytest.fixture(scope="class")
    def macros(self):
        return {"test_assert_equal.sql": macros__test_assert_equal_sql}
    
    # each child class will reimplement 'models' + 'seeds'
    def seeds(self):
        return {}
        
    def models(self):
        return {}

    # actual test sequence
    def test_build_assert_equal(self, project):
        run_dbt(['deps'])
        run_dbt(['build'])    # seed, model, test -- all handled by dbt
