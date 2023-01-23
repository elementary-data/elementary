from datetime import datetime
from typing import Dict, List, Optional

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.clients.slack.slack_message_builder import SlackMessageBuilder
from elementary.monitor.api.tests.schema import TestResultSummarySchema
from elementary.utils.time import convert_utc_time_to_timezone


class SlackReportSummaryMessageBuilder(SlackMessageBuilder):
    def __init__(self) -> None:
        super().__init__()

    def get_slack_message(
        self,
        test_results: List[TestResultSummarySchema],
        bucket_website_url: Optional[str] = None,
    ) -> SlackMessageSchema:
        totals = self._get_test_results_totals(test_results)
        self._add_title_to_slack_alert(totals, bucket_website_url)
        self._add_preview_to_slack_alert(test_results)
        self._add_details_to_slack_alert(test_results)
        return super().get_slack_message()

    def _add_title_to_slack_alert(
        self,
        totals: Dict[str, int],
        bucket_website_url: Optional[str] = None,
    ):
        current_time = convert_utc_time_to_timezone(datetime.utcnow()).strftime(
            "%Y-%m-%d | %H:%M"
        )
        title_blocks = [
            self.create_header_block(
                f":mag: Elementary monitoring report summary ({current_time})"
            ),
        ]

        if bucket_website_url:
            title_blocks.append(
                self.create_text_section_block(
                    f"<{bucket_website_url}|View full report> :arrow_upper_right:"
                )
            )

        title_blocks.append(
            self.create_text_section_block(
                f":white_check_mark: Passed: {totals.get('passed', 0)}      |     :x: Failed: {totals.get('failed', 0)}   |     :Warning: Warning: {totals.get('warning', 0)}    |   :wrench: Schema changes: {totals.get('schema_changes', 0)}"
            )
        )

        title_blocks.append(self.create_divider_block())
        self._add_always_displayed_blocks(title_blocks)

    def _add_preview_to_slack_alert(self, test_results: List[TestResultSummarySchema]):
        owners = []
        tags = []
        subscribers = []
        for test in test_results:
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
            self.create_text_section_block(f"*Tags:* {tags_text}"),
            self.create_text_section_block(f"*Owners:* {owners_text}"),
            self.create_text_section_block(f"*Subscribers:* {subscribers_text}"),
            self.create_empty_section_block(),
            self.create_empty_section_block(),
        ]
        if preview_blocks:
            self._add_blocks_as_attachments(preview_blocks)

    def _add_details_to_slack_alert(self, test_results: List[TestResultSummarySchema]):
        failed_tests_details = []
        warning_tests_details = []
        schema_changes_tests_details = []
        for test in test_results:
            if test.test_type == "schema_change" and test.status != "pass":
                schema_changes_tests_details.extend(
                    self._get_test_result_details_block(test)
                )
            elif test.status in ["error", "fail"]:
                failed_tests_details.extend(self._get_test_result_details_block(test))
            else:
                warning_tests_details.extend(self._get_test_result_details_block(test))

        details_blocks = []
        if failed_tests_details:
            details_blocks.append(self.create_text_section_block(":X: *Failed tests*"))
            details_blocks.append(self.create_divider_block())
            details_blocks.extend(failed_tests_details)
            details_blocks.append(self.create_empty_section_block())

        if warning_tests_details:
            details_blocks.append(self.create_text_section_block(":warning: *Warning*"))
            details_blocks.append(self.create_divider_block())
            details_blocks.extend(warning_tests_details)
            details_blocks.append(self.create_empty_section_block())

        if schema_changes_tests_details:
            details_blocks.append(
                self.create_text_section_block(":wrench: *Schema changes*")
            )
            details_blocks.append(self.create_divider_block())
            details_blocks.extend(schema_changes_tests_details)
            details_blocks.append(self.create_empty_section_block())

        if details_blocks:
            self._add_blocks_as_attachments(details_blocks)

    def _get_test_result_details_block(
        self, test_result: TestResultSummarySchema
    ) -> List[dict]:
        test_name_text = f"{test_result.table_name.lower() if test_result.table_name else ''}{f' ({test_result.column_name.lower()})' if test_result.column_name else ''}"
        test_type_text = f"{test_result.test_name}{f' - {test_result.test_sub_type}' if test_result.test_sub_type != 'generic' else ''}"
        details_blocks = [
            self.create_text_section_block(
                f"{f'*{test_name_text}* | ' if test_name_text else ''}{test_type_text}"
            )
        ]
        if test_result.description:
            details_blocks.append(
                self.create_context_block([f"_Description:_ {test_result.description}"])
            )
        return details_blocks

    @staticmethod
    def _get_test_results_totals(
        test_results: List[TestResultSummarySchema],
    ) -> Dict[str, int]:
        totals = dict(passed=0, failed=0, warning=0, schema_changes=0)
        for test in test_results:
            if test.test_type == "schema_change" and test.status != "pass":
                totals["schema_changes"] += 1
            elif test.status == "pass":
                totals["passed"] += 1
            elif test.status in ["error", "fail"]:
                totals["failed"] += 1
            else:
                totals["warning"] += 1
        return totals
