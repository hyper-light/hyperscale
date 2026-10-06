"""
The soft-state cache of a cluster's membership (AD-52 section 10): the
membership a node last observed, and how stale that observation is.
"""

from .models.cluster_member_id import ClusterMemberId
from .models.cluster_view import EMPTY_CLUSTER_VIEW, ClusterView
from .models.cluster_watch_reply import ClusterWatchReply

MemberAddress = tuple[str, int]


def _parse_address(entry: str) -> MemberAddress:
    host, _, port = entry.rpartition(":")
    return host, int(port)


class ClusterViewCache:
    """The membership a node last observed through a watch (AD-52 sections
    9-10) -- a snapshot, then each change in commit order -- and when.

    Consumers read the view with its staleness and choose: a read that
    must be linearizable goes to ReadIndex instead (section 11); a bounded
    one re-reads past its bound; a best-effort one takes the view as is.
    The view is observed as of when the poll that brought it was SENT: the
    member answered after that, so the view is at least that fresh.

    A healthy watch is never staler than two polls: the view dates from
    when the poll that brought it was sent, that poll took up to its wait
    plus the request's budget to answer, and the next -- sent at once --
    as long again before its answer re-dates the view. A staleness past
    twice (wait + budget) therefore proves the watch cannot reach the
    cluster: disconnected (section 10), the view still served, its
    staleness growing.
    """

    __slots__ = (
        "_view",
        "_observed_at",
        "_disconnected_after_seconds",
        "_snapshots_applied",
        "_events_applied",
    )

    def __init__(self, poll_wait_seconds: float, request_timeout_seconds: float) -> None:
        """
        Args:
            poll_wait_seconds: How long a watch poll waits for a change
            request_timeout_seconds: The budget a poll's request has beyond
                its wait
        """
        self._view = EMPTY_CLUSTER_VIEW
        self._observed_at: float | None = None
        self._disconnected_after_seconds = 2 * (poll_wait_seconds + request_timeout_seconds)
        self._snapshots_applied = 0
        self._events_applied = 0

    def apply(self, reply: ClusterWatchReply, poll_sent_at: float) -> ClusterView:
        """Fold a served watch reply into the view: a snapshot replaces it,
        events advance it in commit order -- an event at or before the
        view's index is already in it and is skipped. Returns the view."""
        view = self._view
        if reply.snapshot:
            view = ClusterView(
                cluster_uuid=reply.cluster_uuid,
                applied_index=reply.applied_index,
                holders={ClusterMemberId.parse(holder).address: holder for holder in reply.holders},
                cohort=frozenset(_parse_address(entry) for entry in reply.cohort),
                voters=frozenset(reply.voters),
                learners=frozenset(reply.learners),
                mode=reply.mode,
            )
            self._snapshots_applied += 1
        elif reply.events:
            holders = dict(view.holders)
            cohort = view.cohort
            voters = view.voters
            learners = view.learners
            mode = view.mode
            for index, kind, detail in reply.events:
                if index <= view.applied_index:
                    continue
                self._events_applied += 1
                match kind:
                    case "claim":
                        holders[ClusterMemberId.parse(detail).address] = detail
                    case "release":
                        released_address = ClusterMemberId.parse(detail).address
                        if holders.get(released_address) == detail:
                            del holders[released_address]
                    case "mode":
                        mode = detail
                    case "resize":
                        # An address the cohort no longer holds is no one's.
                        cohort = frozenset(_parse_address(entry) for entry in detail.split())
                        holders = {address: holder for address, holder in holders.items() if address in cohort}
                    case "configuration":
                        fields = dict(part.split("=", 1) for part in detail.split() if "=" in part)
                        voters = frozenset(voter for voter in fields.get("voters", "").split(",") if voter)
                        learners = frozenset(learner for learner in fields.get("learners", "").split(",") if learner)
            view = ClusterView(
                cluster_uuid=reply.cluster_uuid,
                applied_index=max(view.applied_index, reply.applied_index),
                holders=holders,
                cohort=cohort,
                voters=voters,
                learners=learners,
                mode=mode,
            )
        elif reply.applied_index > view.applied_index or reply.cluster_uuid != view.cluster_uuid:
            view = ClusterView(
                cluster_uuid=reply.cluster_uuid,
                applied_index=max(view.applied_index, reply.applied_index),
                holders=view.holders,
                cohort=view.cohort,
                voters=view.voters,
                learners=view.learners,
                mode=view.mode,
            )
        self._view = view
        if self._observed_at is None or poll_sent_at > self._observed_at:
            self._observed_at = poll_sent_at
        return view

    def read(self, now: float) -> tuple[ClusterView, float]:
        """The view and its staleness in seconds at ``now`` (infinite
        before the first observation)."""
        observed_at = self._observed_at
        return self._view, (float("inf") if observed_at is None else now - observed_at)

    def is_disconnected(self, now: float) -> bool:
        """Whether the watch has provably lost the cluster: the view is
        staler than a healthy watch ever lets it get."""
        observed_at = self._observed_at
        return observed_at is None or now - observed_at > self._disconnected_after_seconds

    @property
    def view(self) -> ClusterView:
        return self._view

    @property
    def disconnected_after_seconds(self) -> float:
        return self._disconnected_after_seconds

    def get_stats(self) -> dict[str, int | float | str | None]:
        return {
            "cluster_uuid": self._view.cluster_uuid,
            "applied_index": self._view.applied_index,
            "holders": len(self._view.holders),
            "snapshots_applied": self._snapshots_applied,
            "events_applied": self._events_applied,
        }
