"""
This file implements the original critical path algorithm (called LongestPath)
for synchronous microservices presented at OSDI'20.
Source: qiu:20 "FIRM: An Intelligent Fine-grained Resource Management
        Framework for SLO-Oriented Microservices"

The tests under tests/unit/critical_path_algorithm_sync_tests.py
demonstrate the limitations of the original version.

The version below fixes the ordering issue demonstrated by the test case
`test_longest_path_sync_only` by moving Line 8 to after Line 13 (see comments).
"""


# 1: procedure LongestPath(G, currentNode)
def longest_path(currentSpan):
    """
    Algorithm 1 Critical Path Extraction
    Require: Microservice execution history graph G
    Attributes: childNodes, lastReturnedChild procedure LongestPath(G, currentNode)
    """
    # 2: path←∅
    path = []
    # 3: path.add(currentNode)
    path.append(currentSpan)
    # 4: if currentNode.childNodes == None then
    if not currentSpan.childSpans:
        # 5: Return path end if
        return path
    # 7: lrc ← currentNode.lastReturnedChild
    sortedChildSpans = sorted(currentSpan.childSpans, key=lambda x: (x.endTime))
    lastChild = sortedChildSpans[-1]
    # ORIGINAL algorithm causes wrong ordering
    # => move Line 8 to after the for each loop (i.e., Line 13)
    # 8: path.extend(LongestPath(G, lrc))
    # path.extend(longest_path(lastChild))
    # 9: for each cn in currentNode.childNodes do
    for child in sortedChildSpans:
        # 10: if cn.happensBefore(lrc) then
        if child.happensBefore(lastChild):
            # 11: path.extend(LongestPath(G, cn))
            path.extend(longest_path(child))
    # FIXED algorithm: Line 8 goes here to fix ordering problem
    path.extend(longest_path(lastChild))
    # 14: Return path
    return path
