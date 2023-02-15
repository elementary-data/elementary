from datetime import datetime
from typing import Dict, List, Optional

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.clients.slack.slack_message_builder import SlackMessageBuilder
from elementary.monitor.data_monitoring.schema import SelectorFilterSchema
from elementary.monitor.fetchers.tests.schema import TestResultSummarySchema
from elementary.utils.time import convert_utc_time_to_timezone


class SlackReportSummaryMessageBuilder(SlackMessageBuilder):
    def __init__(self) -> None:
        super().__init__()

    def get_slack_message(
        self,
        test_results: List[TestResultSummarySchema],
        env: str,
        days_back: int,
        bucket_website_url: Optional[str] = None,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
        include_description: bool = False,
    ) -> SlackMessageSchema:
        self._add_title_to_slack_alert(
            test_results=test_results,
            bucket_website_url=bucket_website_url,
            days_back=days_back,
            env=env,
            filter=filter,
        )
        self._add_preview_to_slack_alert(test_results)
        self._add_details_to_slack_alert(
            test_results=test_results,
            include_description=include_description,
            bucket_website_url=bucket_website_url,
        )
        return super().get_slack_message()

    def _add_title_to_slack_alert(
        self,
        test_results: List[TestResultSummarySchema],
        env: str,
        days_back: int,
        bucket_website_url: Optional[str] = None,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
    ):
        current_time = convert_utc_time_to_timezone(datetime.utcnow()).strftime(
            "%Y-%m-%d | %H:%M"
        )
        env_text = (
            ":construction: Development"
            if env == "dev"
            else ":large_green_circle: Production"
        )
        summary_filter_text = self._get_summary_filter_text(days_back, filter)
        totals = self._get_test_results_totals(test_results)

        title_blocks = [
            self.create_header_block(
                f":mag: Monitoring summary ({current_time} | {env_text})"
            ),
            self.create_text_section_block(summary_filter_text),
        ]

        if bucket_website_url:
            title_blocks.append(
                self.create_text_section_block(
                    f"<{bucket_website_url}|View full report> :arrow_upper_right:"
                )
            )

        title_blocks.append(
            self.create_fields_section_block(
                [
                    f":white_check_mark: Passed: {totals.get('passed', 0)}",
                    f":wrench: Schema changes: {totals.get('schema_changes', 0)}",
                    f":small_red_triangle: Failed: {totals.get('failed', 0)}",
                    f":exclamation: Errors: {totals.get('error', 0)}",
                    f":Warning: Warning: {totals.get('warning', 0)}",
                ]
            )
        )

        title_blocks.append(self.create_divider_block())
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
        days_back_text = (
            f"timeframe: {days_back} day{'s' if days_back > 1 else ''} back"
        )

        return f"_This summary was generated with the following filters - {days_back_text}{f', {selector_text}' if selector_text else ''}_"

    def _add_preview_to_slack_alert(self, test_results: List[TestResultSummarySchema]):
        owners = []
        tags = []
        subscribers = []
        for test in test_results:
            if test.status != "pass":
                owners.extend(test.owners)
                tags.extend(test.tags)
                subscribers.extend(test.subscribers)

        tags_text = self.prettify_and_dedup_list(tags) if tags else "_No tags_"
        owners_text = self.prettify_and_dedup_list(owners) if owners else "_No owners_"
        subscribers_text = (
            self.prettify_and_dedup_list(subscribers)
            if subscribers
            else "_No subscribers_"
        )

        preview_blocks = [
            self.create_text_section_block(":mega: *Attention required* :mega:"),
            self.create_text_section_block(f"*Tags:* {tags_text}"),
            self.create_text_section_block(f"*Owners:* {owners_text}"),
            self.create_text_section_block(f"*Subscribers:* {subscribers_text}"),
            self.create_empty_section_block(),
        ]
        if preview_blocks:
            self._add_blocks_as_attachments(preview_blocks)

    def _add_details_to_slack_alert(
        self,
        test_results: List[TestResultSummarySchema],
        include_description: bool = False,
        bucket_website_url: Optional[str] = None,
    ):
        error_tests_details = []
        failed_tests_details = []
        warning_tests_details = []
        schema_changes_tests_details = []
        for test in test_results:
            if test.test_type == "schema_change" and test.status != "pass":
                schema_changes_tests_details.extend(
                    self._get_test_result_details_block(test, include_description)
                )
            elif test.status == "error":
                error_tests_details.extend(self._get_test_result_details_block(test))
            elif test.status == "fail":
                failed_tests_details.extend(self._get_test_result_details_block(test))
            else:
                warning_tests_details.extend(self._get_test_result_details_block(test))

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

        if schema_changes_tests_details:
            details_blocks.append(
                self.create_text_section_block(":wrench: *Schema changes*")
            )
            details_blocks.append(self.create_divider_block())
            details_blocks.extend(schema_changes_tests_details)

        if error_tests_details:
            details_blocks.append(
                self.create_text_section_block(":exclamation: *Error*")
            )
            details_blocks.append(self.create_divider_block())
            details_blocks.extend(error_tests_details)

        ammount_of_details_blocks = len(details_blocks)
        if details_blocks and ammount_of_details_blocks <= (
            self._MAX_AMMOUNT_OF_ATTACHMENTS - self._MAX_ALERT_PREVIEW_BLOCKS
        ):
            self._add_blocks_as_attachments(details_blocks)
        elif details_blocks and ammount_of_details_blocks > (
            self._MAX_AMMOUNT_OF_ATTACHMENTS - self._MAX_ALERT_PREVIEW_BLOCKS
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
        totals = dict(passed=0, failed=0, error=0, warning=0, schema_changes=0)
        for test in test_results:
            if test.test_type == "schema_change" and test.status != "pass":
                totals["schema_changes"] += 1
            elif test.status == "pass":
                totals["passed"] += 1
            elif test.status == "error":
                totals["error"] += 1
            elif test.status == "fail":
                totals["failed"] += 1
            else:
                totals["warning"] += 1
        return totals
