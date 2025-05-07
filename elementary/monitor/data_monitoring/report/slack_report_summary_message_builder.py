from typing import Dict, List, Optional

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.clients.slack.slack_message_builder import SlackMessageBuilder
from elementary.monitor.api.tests.schema import TestResultSummarySchema
from elementary.monitor.data_monitoring.schema import SelectorFilterSchema

TAG_PREFIX = "#"


class SlackReportSummaryMessageBuilder(SlackMessageBuilder):
    def __init__(self) -> None:
        super().__init__()

    def get_slack_message(
        self,
        test_results: List[TestResultSummarySchema],
        days_back: int,
        env: str,
        bucket_website_url: Optional[str] = None,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
        include_description: bool = False,
        project_name: Optional[str] = None,
    ) -> SlackMessageSchema:
        self.add_title_to_slack_alert(env, project_name)
        self.add_preview_to_slack_alert(
            test_results,
            days_back=days_back,
            bucket_website_url=bucket_website_url,
            filter=filter,
        )
        self.add_details_to_slack_alert(
            test_results=test_results,
            include_description=include_description,
            bucket_website_url=bucket_website_url,
        )
        return super().get_slack_message()

    def add_title_to_slack_alert(self, env: str, project_name: Optional[str] = None):
        context = f"- {project_name} ({env})" if project_name else f"({env})"
        title_blocks = [
            self.create_header_block(f":mag: Monitoring summary {context}"),
            self.create_divider_block(),
        ]
        self._add_always_displayed_blocks(title_blocks)

    @staticmethod
    def _get_summary_filter_text(
        days_back: int,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
    ) -> str:
        selector_text = None
        if filter.tag:
            selector_text = f"tag: {filter.tag}"
        elif filter.model:
            selector_text = f"model: {filter.model}"
        elif filter.owner:
            selector_text = f"owner: {filter.owner}"
        elif filter.invocation_id:
            selector_text = f"invocation_id: {filter.invocation_id}"
        elif filter.invocation_time:
            selector_text = f"invocation_time: {filter.invocation_time}"
        elif filter.last_invocation:
            selector_text = "last_invocation"
        days_back_text = (
            f"timeframe: {days_back} day{'s' if days_back > 1 else ''} back"
        )

        return f"_This summary was generated with the following filters - {days_back_text}{f', {selector_text}' if selector_text else ''}_"

    def add_preview_to_slack_alert(
        self,
        test_results: List[TestResultSummarySchema],
        days_back: int,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
        bucket_website_url: Optional[str] = None,
    ):
        preview_blocks = []

        summary_filter_text = self._get_summary_filter_text(days_back, filter)
        preview_blocks.append(self.create_text_section_block(summary_filter_text))

        if bucket_website_url:
            preview_blocks.append(
                self.create_text_section_block(
                    f"<{bucket_website_url}|View full report> :arrow_upper_right:"
                )
            )

        totals = self._get_test_results_totals(test_results)
        preview_blocks.append(
            self.create_fields_section_block(
                [
                    f":white_check_mark: Passed: {totals.get('passed', 0)}",
                    f":small_red_triangle: Failed: {totals.get('failed', 0)}",
                    f":exclamation: Errors: {totals.get('error', 0)}",
                    f":warning: Warning: {totals.get('warning', 0)}",
                    f":fast_forward: Skipped: {totals.get('skipped', 0)}",
                ]
            )
        )

        preview_blocks_filler = [self.create_empty_section_block()] * (
            self._MAX_ALERT_PREVIEW_BLOCKS - len(preview_blocks)
        )
        preview_blocks.extend(preview_blocks_filler)
        self._add_blocks_as_attachments(preview_blocks)

    def add_details_to_slack_alert(
        self,
        test_results: List[TestResultSummarySchema],
        include_description: bool = False,
        bucket_website_url: Optional[str] = None,
    ):
        error_tests_details = []
        failed_tests_details = []
        warning_tests_details = []
        skipped_tests_details = []
        for test in test_results:
            if test.status == "error":
                error_tests_details.extend(
                    self._get_test_result_details_block(test, include_description)
                )
            elif test.status == "fail":
                failed_tests_details.extend(
                    self._get_test_result_details_block(test, include_description)
                )
            elif test.status == "warn":
                warning_tests_details.extend(
                    self._get_test_result_details_block(test, include_description)
                )
            elif test.status == "skipped":
                skipped_tests_details.extend(
                    self._get_test_result_details_block(test, include_description)
                )
            else:  # test status is "pass" i.e. success
                pass

        details_blocks = []
        if failed_tests_details:
            details_blocks.append(
                self.create_text_section_block(":small_red_triangle: *Failed tests*")
            )
            details_blocks.append(self.create_divider_block())
            details_blocks.extend(failed_tests_details)

        if warning_tests_details:
            details_blocks.append(self.create_text_section_block(":warning: *Warning*"))
            details_blocks.append(self.create_divider_block())
            details_blocks.extend(warning_tests_details)

        if error_tests_details:
            details_blocks.append(
                self.create_text_section_block(":exclamation: *Error*")
            )
            details_blocks.append(self.create_divider_block())
            details_blocks.extend(error_tests_details)

        if skipped_tests_details:
            details_blocks.append(
                self.create_text_section_block(":fast_forward: *Skipped*")
            )
            details_blocks.append(self.create_divider_block())
            details_blocks.extend(skipped_tests_details)

        amount_of_details_blocks = len(details_blocks)
        if details_blocks and amount_of_details_blocks <= (
            self._MAX_AMOUNT_OF_ATTACHMENTS - self._MAX_ALERT_PREVIEW_BLOCKS
        ):
            self._add_blocks_as_attachments(details_blocks)
        elif details_blocks and amount_of_details_blocks > (
            self._MAX_AMOUNT_OF_ATTACHMENTS - self._MAX_ALERT_PREVIEW_BLOCKS
        ):
            too_many_test_results_message = f"_The amount of results exceeded Slack’s limitation. Please {f'<{bucket_website_url}|visit the report>' if bucket_website_url else 'check out the attached report'} to see result’s details{'._' if bucket_website_url else '_ :point_down:'}"
            self._add_blocks_as_attachments(
                [self.create_text_section_block(too_many_test_results_message)]
            )

    def _get_test_result_details_block(
        self, test_result: TestResultSummarySchema, include_description: bool = False
    ) -> List[dict]:
        test_name_text = f"{test_result.table_name.lower() if test_result.table_name else ''}{f' ({test_result.column_name.lower()})' if test_result.column_name else ''}"
        test_type_text = f"{test_result.test_name}{f' - {test_result.test_sub_type}' if test_result.test_sub_type != 'generic' else ''}"
        test_results_counter_text = (
            f"({test_result.results_counter} result{'s' if test_result.results_counter > 1 else ''})"
            if test_result.results_counter
            else ""
        )
        details_blocks = [
            self.create_text_section_block(
                f"{f'*{test_name_text}* | ' if test_name_text else ''}{test_type_text}{f' {test_results_counter_text}' if test_results_counter_text else ''}"
            )
        ]
        if include_description and test_result.description:
            details_blocks.append(
                self.create_context_block([f"_Description:_ {test_result.description}"])
            )
        return details_blocks

    @staticmethod
    def _get_test_results_totals(
        test_results: List[TestResultSummarySchema],
    ) -> Dict[str, int]:
        totals = dict(passed=0, failed=0, error=0, warning=0, skipped=0)
        for test in test_results:
            if test.status == "pass":
                totals["passed"] += 1
            elif test.status == "error":
                totals["error"] += 1
            elif test.status == "fail":
                totals["failed"] += 1
            elif test.status == "warn":
                totals["warning"] += 1
            elif test.status == "skipped":
                totals["skipped"] += 1
        return totals
