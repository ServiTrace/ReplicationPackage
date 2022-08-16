from sb.critical_path_algorithm_async import Span
from sb.critical_path_algorithm_sync import longest_path

import pytest


def test_simple():
    """Basic two-span trace with synchronous invocation."""
    s2 = Span(1, 3, [])
    s1 = Span(0, 6, [s2])
    cp = longest_path(s1)
    assert cp == [s1, s2]


def test_longest_path_sync_only():
    """Scenario where a synchronous invocation is the longest path
    and no asynchronous call is present.
    Adopted from aws_trace_analyzer_test#test_longest_path_sync() by
    removing asynchronous call.
    """
    start_time = 0
    s1_start = start_time
    s2_start = start_time + 10
    s2_end = start_time + 20
    s3_start = start_time + 30
    s3_end = start_time + 40
    s1_end = start_time + 50

    s3 = Span(s3_start, s3_end)
    s2 = Span(s2_start, s2_end,)
    s1 = Span(s1_start, s1_end, [s2, s3])

    cp = longest_path(s1)
    # NOTE: This assertion fails with the original algorithm without
    # moving Line 8 to after the for each loop (i.e., Line 13).
    # Assertion error with original algorithm:
    # [(0, 50), (30, 40), (10, 20)] == [(0, 50), (10, 20), (30, 40)]
    assert cp == [s1, s2, s3]


@pytest.mark.xfail(reason="async spans not supported in original algorithm")
def test_longest_path_sync_with_async_span():
    """Scenario where a synchronous invocation is the longest path
    but an asynchronous call is present.
    Adopted from aws_trace_analyzer_test#test_longest_path_sync().
    """
    start_time = 0
    s1_start = start_time
    s2_start = start_time + 10
    s2_end = start_time + 20
    a_start = start_time + 25
    s3_start = start_time + 30
    a_end = start_time + 35
    s3_end = start_time + 40
    s1_end = start_time + 50

    s3 = Span(s3_start, s3_end)
    a = Span(a_start, a_end)
    s2 = Span(s2_start, s2_end, [a])
    s1 = Span(s1_start, s1_end, [s2, s3])

    cp = longest_path(s1)
    # NOTE: This assertion fails because the calculated longest path
    # contains the asynchronous span `a` (25, 35). However, it should
    # only contain the synchronous invocations s1, s2, and s3.
    # Assertion error:
    # [(0, 50), (30...20), (25, 35)] == [(0, 50), (10, 20), (30, 40)]
    assert cp == [s1, s2, s3]
