import pytest
from dbt.tests.util import run_dbt, check_relations_equal
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_except import (
    seeds__data_except_a_csv,
    seeds__data_except_b_csv,
    seeds__data_except_a_minus_b_csv,
    seeds__data_except_b_minus_a_csv,
    models__data_except_empty_sql,
    models__test_except_a_minus_b_sql,
    models__test_except_b_minus_a_sql,
    models__test_except_a_minus_a_sql,
    models__test_except_a_minus_empty_sql,
    models__test_except_empty_minus_a_sql,
    models__test_except_empty_minus_empty_sql,
)


class BaseExcept(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_except_a.csv": seeds__data_except_a_csv,
            "data_except_b.csv": seeds__data_except_b_csv,
            "data_except_a_minus_b.csv": seeds__data_except_a_minus_b_csv,
            "data_except_b_minus_a.csv": seeds__data_except_b_minus_a_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "data_except_empty.sql": models__data_except_empty_sql,
            "test_except_a_minus_b.sql": models__test_except_a_minus_b_sql,
            "test_except_b_minus_a.sql": models__test_except_b_minus_a_sql,
            "test_except_a_minus_a.sql": models__test_except_a_minus_a_sql,
            "test_except_a_minus_empty.sql": models__test_except_a_minus_empty_sql,
            "test_except_empty_minus_a.sql": models__test_except_empty_minus_a_sql,
            "test_except_empty_minus_empty.sql": models__test_except_empty_minus_empty_sql,
        }

    def test_build_assert_equal(self, project):
        run_dbt(['deps'])
        run_dbt(['build'])

        check_relations_equal(
            project.adapter,
            ["test_except_a_minus_b", "data_except_a_minus_b"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_b_minus_a", "data_except_b_minus_a"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_a_minus_a", "data_except_empty"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_a_minus_empty", "data_except_a"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_empty_minus_a", "data_except_empty"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_empty_minus_empty", "data_except_empty"],
        )


class TestExcept(BaseExcept):
    pass
