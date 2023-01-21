from elementary.clients.api.api import APIClient
from elementary.clients.dbt.dbt_runner import DbtRunner


def _initial_api_client():
    project_dir = "proj_dir"
    profiles_dir = "prof_dir"
    dbt_runner = DbtRunner(project_dir=project_dir, profiles_dir=profiles_dir)
    return APIClient(dbt_runner=dbt_runner)


def test_api_client_set_cache():
    api_client = _initial_api_client()
    api_client.set_run_cache(key="test_field_name", value="cached data")
    assert api_client.run_cache["test_field_name"] == "cached data"
    assert api_client.run_cache["not_existing_key"] is None


def test_api_client_get_cache():
    api_client = _initial_api_client()
    api_client.set_run_cache(key="test_field_name", value="cached data")
    assert api_client.get_run_cache("test_field_name") == "cached data"
    assert api_client.get_run_cache("not_existing_key") is None
