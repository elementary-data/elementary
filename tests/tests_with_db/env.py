from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner


def init(dbt_runner: SubprocessDbtRunner):
    tests_env = Environment(dbt_runner)
    tests_env.clear()
    tests_env.init()


class Environment:
    def __init__(self, dbt_runner: SubprocessDbtRunner):
        self.dbt_runner = dbt_runner

    def clear(self):
        self.dbt_runner.run_operation("elementary_tests.clear_env")

    def init(self):
        self.dbt_runner.run(selector="init")
        self.dbt_runner.run(select="elementary")
