import pytest
from tests.functional.cross_db_utils.base_cross_db_macro import BaseCrossDbMacro
from tests.functional.cross_db_utils.fixture_escape_single_quotes import (
    models__test_escape_single_quotes_quote_sql,
    models__test_escape_single_quotes_backslash_sql,
    models__test_escape_single_quotes_yml,
)


class BaseEscapeSingleQuotesQuote(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_escape_single_quotes.yml": models__test_escape_single_quotes_yml,
            "test_escape_single_quotes.sql": models__test_escape_single_quotes_quote_sql,
        }


class BaseEscapeSingleQuotesBackslash(BaseCrossDbMacro):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_escape_single_quotes.yml": models__test_escape_single_quotes_yml,
            "test_escape_single_quotes.sql": models__test_escape_single_quotes_backslash_sql,
        }


@pytest.mark.only_profile("postgres")
class TestEscapeSingleQuotesPostgres(BaseEscapeSingleQuotesQuote):
    pass


@pytest.mark.only_profile("redshift")
class TestEscapeSingleQuotesRedshift(BaseEscapeSingleQuotesQuote):
    pass


@pytest.mark.only_profile("snowflake")
class TestEscapeSingleQuotesSnowflake(BaseEscapeSingleQuotesBackslash):
    pass


@pytest.mark.only_profile("bigquery")
class TestEscapeSingleQuotesBigQuery(BaseEscapeSingleQuotesBackslash):
    pass
