import json
from typing import Any, Dict, List, Optional

from clients.api.api import APIClient
from monitor.api.tests.schema import TestMetadataSchema, TestUniqueIdType, InvocationSchema


class TestsAPI(APIClient):
    @staticmethod
    def _get_test_sub_type_unique_id(test: TestMetadataSchema) -> str:
        return f"{test.test_unique_id}.{test.test_type}.{test.test_sub_type if test.test_sub_type else 'None'}.{test.table_name if test.table_name else 'None'}.{test.column_name if test.column_name else 'None'}"

    def get_tests_metadata(self, days_back: Optional[int] = 7) -> List[TestMetadataSchema]:
        tests_metadata = json.loads(self.dbt_runner.run_operation(macro_name='get_test_results', macro_args=dict(days_back=days_back))[0])
        return [TestMetadataSchema(**test_metadata) for test_metadata in tests_metadata]

    def get_metrics(
        self,
        tests_metadata: Optional[List[TestMetadataSchema]] = None,
        days_back: Optional[int] = None,
        metrics_sample_limit: int = 5
    ) -> Dict[TestUniqueIdType, Dict[str, Any]]:
        tests: List[TestMetadataSchema] = tests_metadata if tests_metadata is not None else self.get_tests_metadata(days_back=days_back)
        tests = [dict(test) for test in tests]
        return json.loads(self.dbt_runner.run_operation(
            macro_name='get_tests_metrics',
            macro_args=dict(tests=tests, metrics_sample_limit=metrics_sample_limit)
        )[0])

    def get_invocations(
        self,
        invocations_per_test: int = 30
    ) -> Dict[TestUniqueIdType, List[InvocationSchema]]:
        tests_invocations = dict()
        raw_invocations = json.loads(self.dbt_runner.run_operation(
            macro_name='get_tests_invocations',
            macro_args=dict(invocations_per_test=invocations_per_test)
        )[0])
        for sub_test_unique_id, invocations in raw_invocations.items():
            test_invocations = []
            test_invocations_times = json.loads(invocations["invocations_times"])
            test_invocations_ids = json.loads(invocations["ids"])
            test_invocations_affected_rows = json.loads(invocations["affected_rows"])
            test_invocations_statuses = json.loads(invocations["statuses"])
            for index, invocation_id in enumerate(test_invocations_ids):
                test_invocations.append(InvocationSchema(
                    id=invocation_id,
                    time_utc=test_invocations_times[index],
                    status=test_invocations_statuses[index],
                    affected_rows=self._parse_affected_row(affected_rows=test_invocations_affected_rows, index=index)
                ))
            tests_invocations[sub_test_unique_id] = test_invocations
        return tests_invocations
    
    @staticmethod
    def _parse_affected_row(affected_rows: List[str], index: int) -> Optional[int]:
        try:
            return int(affected_rows[index])
        except Exception:
            return None

    def get_total_tests_results(self, tests_metadata: Optional[List[TestMetadataSchema]] = None, days_back: Optional[int] = None):
        tests: List[TestMetadataSchema] = tests_metadata if tests_metadata is not None else self.get_tests_metadata(days_back=days_back)
        totals = dict()
        for test in tests:
            self._update_test_results_totals(
                totals_dict=totals,
                model_unique_id=test.model_unique_id,
                status=test.status
            )
        return totals
    
    def get_total_tests_runs(
        self, 
        tests_metadata: Optional[List[TestMetadataSchema]] = None, 
        tests_invocations: Optional[Dict[TestUniqueIdType, List[InvocationSchema]]] = None,
        invocations_per_test: Optional[int] = None,
        days_back: Optional[int] = None,
    ):
        tests: List[TestMetadataSchema] = tests_metadata if tests_metadata is not None else self.get_tests_metadata(days_back=days_back)
        invocations: Optional[Dict[TestUniqueIdType, List[InvocationSchema]]] = tests_invocations if tests_invocations else self.get_invocations(invocations_per_test=invocations_per_test)
        totals = dict()
        for test in tests:
            test_sub_type_unique_id = self._get_test_sub_type_unique_id(test=test)
            test_invocations = invocations[test_sub_type_unique_id]
            self._update_test_runs_totals(
                totals_dict=totals,
                test=test,
                test_invocations=test_invocations
            )
        return totals
    
    @staticmethod
    def _update_test_runs_totals(totals_dict: dict, test: TestMetadataSchema, test_invocations: List[InvocationSchema]):
        model_unique_id = test.model_unique_id

        if model_unique_id not in totals_dict:
            totals_dict[model_unique_id] = {
                'errors': 0,
                'warnings': 0,
                'resolved': 0,
                'passed': 0
            }
        
        for test_invocation in test_invocations:
            invocation_status = test_invocation.status
            if invocation_status == 'warn':
                totals_status = 'warnings'
            elif invocation_status == 'error' or invocation_status == 'fail':
                totals_status = 'errors'
            elif invocation_status == 'pass':
                totals_status = 'passed'
            else:
                totals_status = None

            if totals_status is not None:
                totals_dict[model_unique_id][totals_status] += 1

    @staticmethod
    def _update_test_results_totals(totals_dict, model_unique_id, status):
        if model_unique_id not in totals_dict:
            totals_dict[model_unique_id] = {
                'errors': 0,
                'warnings': 0,
                'resolved': 0,
                'passed': 0
            }

        if status == 'warn':
            totals_status = 'warnings'
        elif status == 'error' or status == 'fail':
            totals_status = 'errors'
        elif status == 'pass':
            totals_status = 'passed'
        else:
            totals_status = None

        if totals_status is not None:
            totals_dict[model_unique_id][totals_status] += 1
