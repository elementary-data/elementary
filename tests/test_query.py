import pytest
from lineage.query import Query
from lineage.query_context import QueryContext
from sqllineage.core import LineageResult, Table


def create_lineage_result(read, write):
    result = LineageResult()
    if read is not None:
        result.read = {Table(read)}
    if write is not None:
        result.write = {Table(write)}
    return result


@pytest.mark.parametrize("query_text,expected_parsed_result", [
    ("insert into target_table (a, b) (select c, count(*) from source_table group by c);",
     create_lineage_result('source_table', 'target_table')),
    ("insert into target_table (select c, count(*) from source_table group by c);",
     create_lineage_result(None, 'target_table'))  # This syntax is not supported currently, therefore source_table
    # is not identified
])
def test_query_parse_query_text(query_text, expected_parsed_result):
    empty_context = QueryContext()
    reference = Query(query_text, empty_context, 'elementary_db', 'elementary_sc')
    parsed_results = reference._parse_query_text(query_text)
    assert len(parsed_results) == 1
    assert parsed_results[0].read == expected_parsed_result.read
    assert parsed_results[0].write == expected_parsed_result.write
