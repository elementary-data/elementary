import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_cast_bool_to_text import (
    models__test_cast_bool_to_text_sql,
    models__test_cast_bool_to_text_yml,
)


class BaseCastBoolToText(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_cast_bool_to_text.yml": models__test_cast_bool_to_text_yml,
            "test_cast_bool_to_text.sql": models__test_cast_bool_to_text_sql,
        }


class TestCastBoolToText(BaseCastBoolToText):
    pass
