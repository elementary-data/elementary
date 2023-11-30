from collections import defaultdict
from typing import Dict, List, Optional, Union

from elementary.monitor.api.source_freshnesses.schema import (
    SourceFreshnessMetadataSchema,
    SourceFreshnessResultSchema,
    SourceFreshnessRunSchema,
)
from elementary.monitor.api.tests.schema import (
    TestMetadataSchema,
    TestResultSchema,
    TestRunSchema,
)
from elementary.monitor.api.totals_schema import TotalsSchema


def get_total_test_results(
    test_results: Dict[
        Optional[str], List[Union[TestResultSchema, SourceFreshnessResultSchema]]
    ],
) -> Dict[Optional[str], TotalsSchema]:
    test_metadatas = []
    for test_result in test_results.values():
        test_metadatas.extend([result.metadata for result in test_result])

    return _calculate_test_results_totals(test_metadatas)


def get_total_test_runs(
    tests_runs: Dict[
        Optional[str], List[Union[TestRunSchema, SourceFreshnessRunSchema]]
    ]
) -> Dict[Optional[str], TotalsSchema]:
    totals: Dict[Optional[str], TotalsSchema] = defaultdict(TotalsSchema)
    for test_runs in tests_runs.values():
        for test_run in test_runs:
            # It's possible test_runs will be None if we didn't find any invocations associated
            # with this test, in that case it also makes sense to skip it.
            if not test_run.test_runs:
                continue

            test_invocations = test_run.test_runs.invocations
            model_unique_id = test_run.metadata.model_unique_id

            for test_invocation in test_invocations:
                totals[model_unique_id].add_total(test_invocation.status)
    return totals


def _calculate_test_results_totals(
    test_metadatas: List[Union[TestMetadataSchema, SourceFreshnessMetadataSchema]],
) -> Dict[Optional[str], TotalsSchema]:
    totals: Dict[Optional[str], TotalsSchema] = defaultdict(TotalsSchema)
    for test in test_metadatas:
        totals[test.model_unique_id].add_total(test.latest_run_status)
    return totals
