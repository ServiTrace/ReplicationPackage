"""This file implements and explains the algorithm for
critical path extraction supporting asynchronous invocations.

Basic tests available under: tests/unit/critical_path_algorithm_async_test.py

This abstracted version demonstrates the generic and easier-to-understand
algorithm compared to the actual implementation used in the aws_trace_analyzer.py,
which is cluttered with implementation details supporting many special cases.
The unit tests under `tests/unit/aws_trace_analyzer_test.py` cover several cases
but the main testing is done through the integrated validations and
analysis of over 21 million traces.
"""

# Import for alternative implementation (see comment in critical_path below)
# import operator


class Span:
    """Represents a trace span with timestamps (start + end)
    and optional children.
    """
    def __init__(self, startTime, endTime, childSpans=[]):
        self.startTime = startTime
        self.endTime = endTime
        self.childSpans = childSpans

    def __str__(self):
        return f"{self.startTime, self.endTime}"

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        if isinstance(other, Span):
            return (self.startTime == other.startTime
                    and self.endTime == other.endTime
                    and self.childSpans == other.childSpans)
        return False

    # procedure current.HappensBefore(next)
    def happensBefore(self, next):
        """Heuristic to detect if this span (i.e., `self`) happens before the `next` span
        in sequential order.
        Alternative name: endsBefore
        Limitation: can fail for two consecutive spans with overlapping (i.e., identical)
        timestamps, which can happen in fine-grained serverless traces.
        """
        # Return current.endTime <= next.startTime
        return self.endTime <= next.startTime

    # procedure current.IsAsync(next)
    def isAsync(self, next):
        """Heuristic to detect if the `next` span is invoked asynchronously.
        This heuristic can identify obvious asynchronous invocation.
        However, it might miss async calls that end before their parent
        because it is impossible to detect from traces (see qiu:20 FIRM paper).
        Hence, we cannot reliably identify synchronous invocations.
        Conceptually, next.endTime > self.endTime
        would implement this heuristic. However, timestamps with different
        precision (e.g., ms vs µs) can lead to calls being classified as
        async by mistake. Using a margin of precision error (e.g., 999µs)
        can mitigate this issue.
        Sample implementation of comparison t2 > t1 using margin 𝛆: t2 - t1 + 𝛆 < 0
        Caveat: This can result in negative time differences in a latency breakdown!
        Suggestions: Traces could define labels for synchronous and asynchronous
        parent-child relationships, which is discussed for OpenTelemetry:
        https://github.com/open-telemetry/opentelemetry-specification/issues/65
        """
        # Return next.endTime > current.endTime
        # return next.endTime > self.endTime
        MARGIN = 1
        return self.endTime - next.endTime + MARGIN < 0


# 1: procedure T.CriticalPath(S, currentSpan)
def critical_path(S, currentSpan):
    """Algorithm 1: Critical path extraction supporting asynchronous invocations.
    The line numbers and identical code from the paper are annotated for easy comparison.
    This algorithm is a modified version of the weighted longest-path algorithm
    proposed for microservices in OSDI'20: qiu:20: "FIRM: An Intelligent Fine-grained
    Resource Management Framework for SLO-Oriented Microservices."
    It addresses the three challenges (C1-C3) explained below.

    Algorithm description:
    This critical path algorithm adapted for supporting asynchronous invocation works by
    iteratively building the longest path (i.e., critical path) from
    the start of the trace (i.e., span with earliest startTime)
    to the end of the trace (i.e., span with latest endTime).
    It leverages recursion to follow into child spans under two conditions:
    a) For asynchronous calls, a child span is only followed
       if it is connected to the end of the trace.
    b) For synchronous calls, a child span is only followed
       if there is not already a longer asynchronous call present.
    These two conditions are easy to check using the auxillary stack S.

    Challenges (C) and solutions (=>):
    * C1: Wrong order of child spans in original version
      * => Fixed in updated version of the algorithm by moving Line 8 in qiu:20 to after Line 13.
           Line 8 `path.append(...)` produced a wrong order within child spans in critical path.
           Moving this line after the for loop (i.e., after line 13) fixed this issue..
    * C2: Asynchronous invocation cannot be detected reliably from traces
      * => H1: Heuristic1 detects obvious async invocations.
           Limitation: Cannot detect an asynchronous invocation that ends before its parent
           (same case as Sync1 in Fig. 4 in the ServiTrace paper).
      * => Conditional recursion with checks whether the child span is connected
           to the end of the trace clarifies which of potentially many async child spans to follow.
    * C3: Timing in fine-grained traces
      * a) different services can report timestamps in different resolutions (e.g., ms vs µs)
      * b) insufficient timestamp resolution for consecutive fine-grained spans leads to spans
           with a duration of 0 milliseconds (ms).
      * c) clock shifts can occur in large-scale distributed systems with timestamps originating
           from many different serverless services.
      * => H2: Heuristic2 fixes the order of 0-ms trace span when adjacent to >0-ms span by
               using the startTime as secondary sort key when primarily sorting by endTime.
            Limitation: Cannot detect two consecutive 0-ms spans.
      * => H3: Heuristic3 implements time comparisons using a margin 𝛆 when comparing timestamps.
               For example: t1 < t2 can be implemented as 0 < t2 - t1 + 𝛆
               The AWS implementation also considers the topological trace information, which takes
               precedence.

    Input:
    S: stack with all parent spans of the span with the latest endTime from
        a serverless execution trace T with span attributes startTime, endTime, and childSpans.
        Such a stack can be obtained by following the parent relationships from the end to the start
        of the trace. This pre-processing can potentially be done during trace correlation.
    currentSpan: currently active span (default: start of trace defined by earliest startTime).

    Output:
    List of critical path from the start to the end of the trace.
    """
    # 2: path ← [currentSpan]
    path = [currentSpan]
    # 3: if S.top() == currentSpan then
    if S and S[-1] == currentSpan:
        # 4: S.pop()
        S.pop()

    # 5: if currentSpan.childSpans == None then
    if not currentSpan.childSpans:
        # 6: Return path
        return path

    # Heuristic: Sorting the child spans by both endTime and startTime mitigates the issue
    # of overlapping traces with fine-grained serverless tracing because it can happen that
    # a trace span has a duration of 0 milliseconds (ms). Adding the startTime as secondary
    # sort key correctly handles these cases where one 0-ms trace adjoins another >0-ms trace.
    # 7: sortedChildS pans ← sortAscending(currentSpan.childSpans, by=[endTime, startTime])
    sortedChildSpans = sorted(currentSpan.childSpans, key=lambda x: (x.endTime, x.startTime))
    # Alternative Python implementation using `operator`:
    # sortedChildSpans = currentSpan.childSpans.sort(key = operator.itemgetter(1, 0))
    # 8: lastChild ← sortedChildSpans.last
    lastChild = sortedChildSpans[-1]
    # 9: for each child in sortedChildSpans do
    for child in sortedChildSpans:
        # Handle child with recursion conditions:
        # a) async: Check against call stack for asynchronous calls by only following calls
        #           that are connected to the end node with the latest timestamp.
        # b) sync: i) Only recurse into child spans that happen before the last child.
        #             This situation represents two sequential spans below a common parent.
        #          ii) Only recurse into synchronous calls if there is not already
        #          a longer asynchronous call present.
        # 10: if (currentSpan.IsAsync(lastChild) and S.top() == lastChild) or
        #        (not currentSpan.IsAsync(lastChild) and
        #        child.HappensBefore(lastChild) and not currentSpan.IsAsync(path.last)) then
        if (currentSpan.isAsync(child) and S and S[-1] == child) or \
           (not currentSpan.isAsync(child) and child.happensBefore(lastChild) and not currentSpan.isAsync(path[-1])):  # noqa: E501
            # 11: path.extend(CriticalPath(S,child))
            path.extend(critical_path(S, child))

    # Handle lastChild with recursion conditions:
    # a) async: Check against call stack for asynchronous calls by only following calls
    #           that are connected to the end node with the latest timestamp.
    # b) sync: Only recurse into synchronous calls if there is not already
    #          a longer asynchronous call present. Same condition as Line 10
    # 12: if (currentSpan.IsAsync(lastChild) and S.top() == lastChild) or
    #        (not currentSpan.IsAsync(lastChild) and not currentSpan.IsAsync(path.last) then
    if (currentSpan.isAsync(lastChild) and S and S[-1] == lastChild) or \
       (not currentSpan.isAsync(lastChild) and not currentSpan.isAsync(path[-1])):  # noqa: E501
        # 13: path.extend(CriticalPath(S, lastChild))
        path.extend(critical_path(S, lastChild))
    # ALTERNATIVE implementation that clarifies the two cases a) async and b) sync case
    # using if/else but it takes too much visual space.
    # if currentSpan.isAsync(lastChild):
    #     if S and S[-1] == lastChild:
    #         path.extend(critical_path(S, lastChild))
    # else:
    #     if not currentSpan.isAsync(path[-1]):
    #         path.extend(critical_path(S, lastChild))
    # 14: Return path
    return path
