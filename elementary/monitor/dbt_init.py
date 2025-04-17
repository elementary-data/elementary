from elementary.clients.dbt.factory import create_dbt_runner
from elementary.monitor import dbt_project_utils


class DBTInit:
    """
    Class to handle the initialization of dbt for the Elementary CLI.
    This can contain all dbt static setup to avoid pulling in runtime dependencies or any other information from internet
    that is internally used by edr.
    """

    def setup_internal_dbt_packages(self):
        """
        Run dbt deps to install internal dbt packages if needed.
        It intentionally does not use self.config.run_dbt_deps_if_needed parameter in create_dbt_runner to ensure
        that dbt deps is always run when setting up the internal dbt packages.
        """
        dbt_runner = create_dbt_runner(dbt_project_utils.CLI_DBT_PROJECT_PATH)
        return dbt_runner.deps()
