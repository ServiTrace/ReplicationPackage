from sb.critical_path_algorithm_async import Span, critical_path


def test_simple():
    """Basic two-span trace with synchronous invocation."""
    s2 = Span(1, 3, [])
    s1 = Span(0, 6, [s2])
    stack = [s1]
    cp = critical_path(stack, s1)
    assert cp == [s1, s2]


def test_longest_path_async():
    """Scenario where an asynchronous invocation is the longest path.
    Adopted from aws_trace_analyzer_test#test_longest_path_async()
    """
    start_time = 0
    s1_start = start_time
    s2_start = start_time + 10
    s2_end = start_time + 20
    s_start = start_time + 30
    s_end = start_time + 40
    s1_end = start_time + 50
    s3_start = start_time + 70
    s3_end = start_time + 80

    s3 = Span(s3_start, s3_end)
    s2 = Span(s2_start, s2_end, [s3])
    s = Span(s_start, s_end)
    s1 = Span(s1_start, s1_end, [s2, s])

    stack = [s3, s2, s1]
    cp = critical_path(stack, s1)
    assert cp == [s1, s2, s3]

    # Source from: aws_trace_analyzer_test#test_longest_path_async()
    # segments = [
    #     ('s1', s1_start, s1_end),
    #     ('s2', s2_start, s2_end),
    #     ('s', s_start, s_end),
    #     ('s3', s3_start, s3_end)
    # ]

    # G = nx.DiGraph()
    # for (id, start_time, end_time) in segments:
    #     s1 = {'id': id, 'start_time': start_time, 'end_time': end_time}
    #     node_attr = {'doc': s1, 'duration': duration(s1)}
    #     G.add_node(s1['id'], **node_attr)
    # G.add_edge('s1', 's2')
    # G.add_edge('s1', 's')
    # G.add_edge('s2', 's3')

    # G.graph['call_stack'] = call_stack(G, 's3')
    # assert ['s1', 's2', 's3'] == longest_path(G, 's1')


def test_longest_path_async_non_last_child():
    """Scenario to test whether asynchronous calls of
    non-last child spans are recognized.
    """
    # Visual trace graph:
    # p  (0     10)
    # a1     (5         15)
    # a11                           (20             30)
    # a2                    (16         21)
    a11 = Span(20, 30)
    a1 = Span(5, 15, [a11])
    a2 = Span(16, 21)
    p = Span(0, 10, [a1, a2])

    stack = [a11, a1, p]
    cp = critical_path(stack, p)
    assert cp == [p, a1, a11]


def test_longest_path_async_non_last_child_overlapping():
    """Scenario to test whether asynchronous calls of
    non-last child spans (e.g., a1) are recognized when
    they do NOT `happensBefore` the last returning child.
    This test covers an unhandled scenario for the simplified
    algorithm. A more complicated recurse condition is provided
    in the source code to support this case.
    """
    # Visual trace graph:
    # p  (0          10)
    # a1      (5                 20)
    # a11                                 (25     30)
    # a2                    (15      21)
    a11 = Span(25, 30)
    a1 = Span(5, 20, [a11])
    a2 = Span(15, 21)
    p = Span(0, 10, [a1, a2])

    stack = [a11, a1, p]
    cp = critical_path(stack, p)
    assert cp == [p, a1, a11]


def test_longest_path_sync():
    """Scenario where a synchronous invocation is the longest path
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

    stack = [s1]
    cp = critical_path(stack, s1)
    assert cp == [s1, s2, s3]

    # Source from: aws_trace_analyzer_test#test_longest_path_sync()
    # segments = [
    #     ('s1', s1_start, s1_end),
    #     ('s2', s2_start, s2_end),
    #     ('a', a_start, a_end),
    #     ('s3', s3_start, s3_end)
    # ]

    # G = nx.DiGraph()
    # G.graph['start'] = 's1'
    # G.graph['end'] = 's1'
    # for (id, start_time, end_time) in segments:
    #     s1 = {'id': id, 'start_time': start_time, 'end_time': end_time}
    #     node_attr = {'doc': s1, 'duration': duration(s1)}
    #     G.add_node(s1['id'], **node_attr)
    # G.add_edge('s1', 's2')
    # G.add_edge('s2', 'a')
    # G.add_edge('s1', 's3')

    # G.graph['call_stack'] = call_stack(G, 's1')
    # assert ['s1', 's2', 's3'] == longest_path(G, 's1')


# Smoke testing:
# Run as python tests/unit/async_critical_path_algorithm_test.py

# s2 = Span(1, 3, [])
# s1 = Span(0, 6, [s2])

# print(s1)

# stack = [s1]
# cp = critical_path(stack, s1)
# print(*cp, sep=", ")
