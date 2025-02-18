from enum import Enum


class AlertField(str, Enum):
    TABLE = "table"
    COLUMN = "column"
    DESCRIPTION = "description"
    OWNERS = "owners"
    TAGS = "tags"
    SUBSCRIBERS = "subscribers"
    RESULT_MESSAGE = "result_message"
    TEST_PARAMS = "test_parameters"
    TEST_QUERY = "test_query"
    TEST_RESULTS_SAMPLE = "test_results_sample"
