import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_string_literal import (
    models__test_string_literal_sql,
    models__test_string_literal_yml,
)


class BaseStringLiteral(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_string_literal.yml": models__test_string_literal_yml,
            "test_string_literal.sql": models__test_string_literal_sql,
        }


class TestStringLiteral(BaseStringLiteral):
    pass
