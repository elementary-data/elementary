from collections import defaultdict
import json
from typing import Dict, List, Optional

from clients.api.api import APIClient
from monitor.api.tests.schema import RawTestMetadataSchema
from monitor.test_result import TestResult


class TestsAPI(APIClient):
    def get_tests_metadata(self):
        raw_tests_results = json.loads(self.dbt_runner.run_operation(macro_name='get_test_results')[0])
        raw_tests_data = []
        tests = defaultdict(list)
        if raw_tests_results:
            for raw_test_result in raw_tests_results:
                raw_test_data = RawTestMetadataSchema(**raw_test_result)
                raw_tests_data.append(raw_test_data)
                test_result = TestResult.create_test_result_from_dict(dict(raw_test_data))
                if test_result:
                    model_unique_id = test_result.model_unique_id
                    tests[model_unique_id].append(test_result.to_test_result_api_dict())
        return dict(
            raw_tests=raw_tests_data,
            tests=tests,
            count=len(raw_tests_results)
        )

    def get_metrics(self, raw_tests_data: Optional[List[RawTestMetadataSchema]] = None):
        tests: List[RawTestMetadataSchema] = raw_tests_data if raw_tests_data else self.get_tests_metadata().get('raw_tests', [])
        tests = [dict(test) for test in tests]
        metrics = self.dbt_runner.run_operation(macro_name='get_metrics', macro_args=dict(tests=tests))
        breakpoint()
        pass

    def get_invocations(self):
        pass

    def get_total_tests_results(self, raw_tests_data: Optional[List[RawTestMetadataSchema]] = None):
        tests: List[RawTestMetadataSchema] = raw_tests_data if raw_tests_data else self.get_tests_metadata().get('raw_tests', [])
        totals = dict()
        for test in tests:
            self._update_test_results_totals(
                totals_dict=totals,
                model_unique_id=test.model_unique_id,
                days_diff=test.days_diff,
                status=test.status
            )
        return totals
    
    def get_total_tests_runs(self, raw_tests_data: Optional[List[RawTestMetadataSchema]] = None):
        tests: List[RawTestMetadataSchema] = raw_tests_data if raw_tests_data else self.get_tests_metadata().get('raw_tests', [])
        totals = dict()
        for test in tests:
            self._update_test_runs_totals(
                totals_dict=totals,
                test=test
            )
        return totals
    
    @staticmethod
    def _update_test_runs_totals(totals_dict: dict, test: RawTestMetadataSchema):
        empty_totals = {
            'errors': 0,
            'warnings': 0,
            'resolved': 0,
            'passed': 0
        }
        model_unique_id = test.model_unique_id
        days_diff = test.days_diff

        if model_unique_id not in totals_dict:
            totals_dict[model_unique_id] = {
                '1d': {**empty_totals},
                '7d': {**empty_totals},
                '30d': {**empty_totals}
            }
        total_keys = []
        if days_diff < 1:
            total_keys.append('1d')
        if days_diff < 7:
            total_keys.append('7d')
        if days_diff < 30:
            total_keys.append('30d')
        
        for test_run in test.test_runs:
            run_status = test_run["status"]
            if run_status == 'warn':
                totals_status = 'warnings'
            elif run_status == 'error' or run_status == 'fail':
                totals_status = 'errors'
            elif run_status == 'pass':
                totals_status = 'passed'
            else:
                totals_status = None

            if totals_status is not None:
                for key in total_keys:
                    totals_dict[model_unique_id][key][totals_status] += 1

    @staticmethod
    def _update_test_results_totals(totals_dict, model_unique_id, days_diff, status):
        empty_totals = {
            'errors': 0,
            'warnings': 0,
            'resolved': 0,
            'passed': 0
        }
        if model_unique_id not in totals_dict:
            totals_dict[model_unique_id] = {
                '1d': {**empty_totals},
                '7d': {**empty_totals},
                '30d': {**empty_totals}
            }
        total_keys = []
        if days_diff < 1:
            total_keys.append('1d')
        if days_diff < 7:
            total_keys.append('7d')
        if days_diff < 30:
            total_keys.append('30d')

        if status == 'warn':
            totals_status = 'warnings'
        elif status == 'error' or status == 'fail':
            totals_status = 'errors'
        elif status == 'pass':
            totals_status = 'passed'
        else:
            totals_status = None

        if totals_status is not None:
            for key in total_keys:
                totals_dict[model_unique_id][key][totals_status] += 1
