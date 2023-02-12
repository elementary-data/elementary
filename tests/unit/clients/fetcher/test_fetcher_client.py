from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.clients.fetcher.fetcher import FetcherClient


def _initial_fetcher_client():
    project_dir = "proj_dir"
    profiles_dir = "prof_dir"
    dbt_runner = DbtRunner(project_dir=project_dir, profiles_dir=profiles_dir)
    return FetcherClient(dbt_runner=dbt_runner)


def test_fetcher_client_set_cache():
    fetcher_client = _initial_fetcher_client()
    fetcher_client.set_run_cache(key="test_field_name", value="cached data")
    assert fetcher_client.run_cache["test_field_name"] == "cached data"
    assert fetcher_client.run_cache["not_existing_key"] is None


def test_fetcher_client_get_cache():
    fetcher_client = _initial_fetcher_client()
    fetcher_client.set_run_cache(key="test_field_name", value="cached data")
    assert fetcher_client.get_run_cache("test_field_name") == "cached data"
    assert fetcher_client.get_run_cache("not_existing_key") is None
