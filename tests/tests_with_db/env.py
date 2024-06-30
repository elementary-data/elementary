from elementary.clients.dbt.dbt_runner import DbtRunner


def init(dbt_runner: DbtRunner):
    tests_env = Environment(dbt_runner)
    tests_env.clear()
    tests_env.init()


class Environment:
    def __init__(self, dbt_runner: DbtRunner):
        self.dbt_runner = dbt_runner

    def clear(self):
        self.dbt_runner.run_operation("elementary_tests.clear_env")

    def init(self):
        self.dbt_runner.run(selector="init")
        self.dbt_runner.run(select="elementary")
