import pytest
import os

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="apache_spark", type=str)


# Using @pytest.mark.skip_adapter('apache_spark') uses the 'skip_by_adapter_type'
# autouse fixture below
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "skip_profile(profile): skip test for the given profile",
    )
    config.addinivalue_line(
        "markers",
        "only_profile(profile): only test the given profile",
    )


@pytest.fixture(scope="session")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    if profile_type == "postgres":
        target = postgres_target()
    elif profile_type == "redshift":
        target = redshift_target()
    elif profile_type == "snowflake":
        target = snowflake_target()
    elif profile_type == "bigquery":
        target = bigquery_target()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")
    return target


def postgres_target():
    return {
        "type": "postgres",
        "host": os.getenv('POSTGRES_TEST_HOST'),
        "user": os.getenv('POSTGRES_TEST_USER'),
        "pass": os.getenv('POSTGRES_TEST_PASS'),
        "port": int(os.getenv('POSTGRES_TEST_PORT')),
        "dbname": os.getenv('POSTGRES_TEST_DBNAME'),
    }


def redshift_target():
    return {
        "type": "redshift",
        "host": os.getenv('REDSHIFT_TEST_HOST'),
        "user": os.getenv('REDSHIFT_TEST_USER'),
        "pass": os.getenv('REDSHIFT_TEST_PASS'),
        "port": int(os.getenv('REDSHIFT_TEST_PORT')),
        "dbname": os.getenv('REDSHIFT_TEST_DBNAME'),
    }


def bigquery_target():
    return {
        "type": "bigquery",
        "method": "service-account",
        "keyfile": os.getenv('BIGQUERY_SERVICE_KEY_PATH'),
        "project": os.getenv('BIGQUERY_TEST_DATABASE'),
    }


def snowflake_target():
    return {
        "type": "snowflake",
        "account": os.getenv('SNOWFLAKE_TEST_ACCOUNT'),
        "user": os.getenv('SNOWFLAKE_TEST_USER'),
        "password": os.getenv('SNOWFLAKE_TEST_PASSWORD'),
        "role": os.getenv('SNOWFLAKE_TEST_ROLE'),
        "database": os.getenv('SNOWFLAKE_TEST_DATABASE'),
        "warehouse": os.getenv('SNOWFLAKE_TEST_WAREHOUSE'),
    }


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip("skipped on '{profile_type}' profile")


@pytest.fixture(autouse=True)
def only_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("only_profile"):
        for only_profile_type in request.node.get_closest_marker("only_profile").args:
            if only_profile_type != profile_type:
                pytest.skip("skipped on '{profile_type}' profile")
