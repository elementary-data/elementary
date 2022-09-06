import pytest
from dbt.tests.util import run_dbt, check_relations_equal
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_intersect import (
    seeds__data_intersect_a_csv,
    seeds__data_intersect_b_csv,
    seeds__data_intersect_a_overlap_b_csv,
    models__data_intersect_empty_sql,
    models__test_intersect_a_overlap_b_sql,
    models__test_intersect_b_overlap_a_sql,
    models__test_intersect_a_overlap_a_sql,
    models__test_intersect_a_overlap_empty_sql,
    models__test_intersect_empty_overlap_a_sql,
    models__test_intersect_empty_overlap_empty_sql,
)


class BaseIntersect(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_intersect_a.csv": seeds__data_intersect_a_csv,
            "data_intersect_b.csv": seeds__data_intersect_b_csv,
            "data_intersect_a_overlap_b.csv": seeds__data_intersect_a_overlap_b_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "data_intersect_empty.sql": models__data_intersect_empty_sql,
            "test_intersect_a_overlap_b.sql": models__test_intersect_a_overlap_b_sql,
            "test_intersect_b_overlap_a.sql": models__test_intersect_b_overlap_a_sql,
            "test_intersect_a_overlap_a.sql": models__test_intersect_a_overlap_a_sql,
            "test_intersect_a_overlap_empty.sql": models__test_intersect_a_overlap_empty_sql,
            "test_intersect_empty_overlap_a.sql": models__test_intersect_empty_overlap_a_sql,
            "test_intersect_empty_overlap_empty.sql": models__test_intersect_empty_overlap_empty_sql,
        }

    def test_build_assert_equal(self, project):
        run_dbt(['deps'])
        run_dbt(['build'])

        check_relations_equal(
            project.adapter,
            ["test_intersect_a_overlap_b", "data_intersect_a_overlap_b"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_b_overlap_a", "data_intersect_a_overlap_b"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_a_overlap_a", "data_intersect_a"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_a_overlap_empty", "data_intersect_empty"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_empty_overlap_a", "data_intersect_empty"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_empty_overlap_empty", "data_intersect_empty"],
        )


class TestIntersect(BaseIntersect):
    pass
