from collections import defaultdict
from typing import Dict, List, Union

from elementary.monitor.api.source_freshnesses.schema import (
    SourceFreshnessResultSchema,
    SourceFreshnessRunSchema,
)
from elementary.monitor.api.tests.schema import TestResultSchema, TestRunSchema
from elementary.monitor.api.totals_schema import TotalsSchema


def get_total_test_results(
    test_results: Dict[str, List[Union[TestResultSchema, SourceFreshnessResultSchema]]],
) -> Dict[str, TotalsSchema]:
    totals: Dict[str, TotalsSchema] = defaultdict(TotalsSchema)
    for key, test_result in test_results.items():
        for result in test_result:
            # count by the key of the tests_results
            totals[key].add_total(result.metadata.latest_run_status)

    return totals


def get_total_test_runs(
    tests_runs: Dict[str, List[Union[TestRunSchema, SourceFreshnessRunSchema]]]
) -> Dict[str, TotalsSchema]:
    totals: Dict[str, TotalsSchema] = defaultdict(TotalsSchema)
    for key, test_runs in tests_runs.items():
        for test_run in test_runs:
            # It's possible test_runs will be None if we didn't find any invocations associated
            # with this test, in that case it also makes sense to skip it.
            if not test_run.test_runs:
                continue

            test_invocations = test_run.test_runs.invocations

            for test_invocation in test_invocations:
                # count by the key of the tests_runs
                totals[key].add_total(test_invocation.status)
    return totals
