"""
ResultRowCursor — the child-side reader of a process's live result log.

Event-triggered faults (``SimulationCoordinator.schedule_on_event``)
need the coordinator to SEE a watched child's milestones while the run
is in flight, not only at shutdown. Child entries publish their
milestones by appending to the list they handed ``ChildContext.set_result``;
this cursor ships each appended row exactly once, at the barrier that
follows the window in which it was appended.
"""


class ResultRowCursor:
    """Track how much of a child's result log has been reported.

    The cursor binds to the list object the entry published: an entry
    that replaces its result with a NEW list (``set_result`` again)
    restarts the cursor on that list, so no row of the live log is ever
    skipped or reported twice. A result that is not a list (or none
    yet) has no rows to report.
    """

    __slots__ = ("_reported_log", "_reported_row_count")

    def __init__(self) -> None:
        self._reported_log: list | None = None
        self._reported_row_count = 0

    def drain(self, result) -> list:
        """Return the rows appended to ``result`` since the last drain."""
        if not isinstance(result, list):
            return []
        if result is not self._reported_log:
            self._reported_log = result
            self._reported_row_count = 0
        new_rows = result[self._reported_row_count :]
        self._reported_row_count = len(result)
        return new_rows
