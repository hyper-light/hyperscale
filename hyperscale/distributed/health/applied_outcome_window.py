"""``AppliedOutcomeWindow`` -- which workflows' H8 outcomes a manager has
already counted, so each terminated workflow is one Bernoulli observation."""

from __future__ import annotations


class AppliedOutcomeWindow:
    """The workflow ids whose AD-26 H8 outcome this manager has applied to
    its ``HierarchicalAlphaTuner``, each held for ``retention_seconds``.

    One workflow terminates once, but its ``ExtensionOutcomeEvent``
    reaches a manager many times: every manager re-broadcasts it
    λ·log(n+1) times over ``#|o`` and the leader hears its own event
    echoed back. Counting each copy would weight a class's posterior
    by how often gossip happened to deliver its outcomes. ``admit``
    answers "first copy?" so the tuner sees each workflow once, and
    the manager re-arms dissemination only for a first copy, so a copy
    arriving after a peer's buffer evicted the event cannot start a
    second epidemic while the id is held.

    Ids are kept in insertion order and dropped from the front once
    expired, so the window holds only the outcomes of the last
    ``retention_seconds`` (amortised O(1) per call). Not thread-safe:
    the manager serialises outcome handling on its event loop.
    """

    def __init__(self, retention_seconds: float) -> None:
        self._retention_seconds: float = retention_seconds
        self._expiry_by_workflow_id: dict[str, float] = {}

    def admit(self, workflow_id: str, now: float) -> bool:
        """Record ``workflow_id`` and return True when it is not already
        held; return False for a repeat inside its retention."""
        self._drop_expired(now)
        if workflow_id in self._expiry_by_workflow_id:
            return False
        self._expiry_by_workflow_id[workflow_id] = now + self._retention_seconds
        return True

    def _drop_expired(self, now: float) -> None:
        """Drop held ids from the oldest while their retention has ended.
        Every id gets the same retention from a monotonic clock, so
        insertion order is expiry order and the scan stops at the first
        live id."""
        while self._expiry_by_workflow_id:
            workflow_id, expiry = next(iter(self._expiry_by_workflow_id.items()))
            if expiry > now:
                return
            del self._expiry_by_workflow_id[workflow_id]

    def __len__(self) -> int:
        return len(self._expiry_by_workflow_id)
