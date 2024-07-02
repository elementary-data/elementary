import json
import uuid

import pytest

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner


class BaseDbtRunnerTest:
    def test_run_operation(self, custom_dbt_runner: BaseDbtRunner):
        result = self._run_query(
            custom_dbt_runner, "select 1 as bla union all select 2 as bla"
        )
        assert result == [{"bla": 1}, {"bla": 2}]

    def test_ls(self, custom_dbt_runner: BaseDbtRunner):
        result = custom_dbt_runner.ls()
        assert isinstance(result, list)
        assert "elementary_tests.one" in result

    def test_seed(self, custom_dbt_runner: BaseDbtRunner):
        result = custom_dbt_runner.seed(select="one")
        assert result is True

    def test_run(self, custom_dbt_runner: BaseDbtRunner):
        test_marker = str(uuid.uuid4())
        result = custom_dbt_runner.run(select="one", vars={"test_marker": test_marker})
        assert result is True

        invocations = self._run_query(
            custom_dbt_runner,
            f"""select * from {{{{ ref("dbt_invocations") }}}} where vars ilike '%{test_marker}%'""",
        )
        assert len(invocations) == 1

        invocation = invocations[0]
        assert invocation["command"] == "run"
        assert invocation["selected"] == json.dumps(["one"])

        result = custom_dbt_runner.run(select="fail_model")
        assert result is False

    def test_test(self, custom_dbt_runner: BaseDbtRunner):
        test_marker = str(uuid.uuid4())
        result = custom_dbt_runner.test(select="one", vars={"test_marker": test_marker})
        assert result is True

        invocations = self._run_query(
            custom_dbt_runner,
            f"""select * from {{{{ ref("dbt_invocations") }}}} where vars ilike '%{test_marker}%'""",
        )
        assert len(invocations) == 1

        invocation = invocations[0]
        assert invocation["command"] == "test"
        assert invocation["selected"] == json.dumps(["one"])

    @staticmethod
    def _run_query(dbt_runner: SubprocessDbtRunner, query: str):
        return json.loads(
            dbt_runner.run_operation(
                "elementary.render_run_query", macro_args={"prerendered_query": query}
            )[0]
        )


class TestSubprocessDbtRunner(BaseDbtRunnerTest):
    @pytest.fixture
    def custom_dbt_runner(self, target, project_dir_copy):
        return SubprocessDbtRunner(
            project_dir_copy,
            target=target,
            vars={
                "debug_logs": True,
            },
            raise_on_failure=False,
        )
