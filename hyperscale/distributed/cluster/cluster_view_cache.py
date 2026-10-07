"""
The soft-state cache of a cluster's membership (AD-52 section 10): the
membership a node last observed, and how stale that observation is.
"""

from collections.abc import Callable, Iterable

from .models.cluster_member_id import ClusterMemberId
from .models.cluster_view import EMPTY_CLUSTER_VIEW, ClusterView
from .models.cluster_watch_reply import ClusterWatchReply

MemberAddress = tuple[str, int]
ClusterViewFields = dict[str, dict[MemberAddress, str] | frozenset[MemberAddress] | frozenset[str] | str | None]


def _parse_address(entry: str) -> MemberAddress:
    host, _, port = entry.rpartition(":")
    return host, int(port)


def _parse_cohort(entries: Iterable[str]) -> frozenset[MemberAddress]:
    """The cohort's addresses from their ``host:port`` entries."""
    return frozenset(_parse_address(entry) for entry in entries)


def _parse_members(text: str) -> frozenset[str]:
    """The non-empty names of a comma-separated member list."""
    return frozenset(member for member in text.split(",") if member)


def _redated_view(view: ClusterView, reply: ClusterWatchReply) -> ClusterView:
    """The view re-dated to an event-less reply's index or cluster; the
    same view when the reply brings neither."""
    if reply.applied_index > view.applied_index or reply.cluster_uuid != view.cluster_uuid:
        return ClusterView(
            cluster_uuid=reply.cluster_uuid,
            applied_index=max(view.applied_index, reply.applied_index),
            holders=view.holders,
            cohort=view.cohort,
            voters=view.voters,
            learners=view.learners,
            mode=view.mode,
        )
    return view


def _apply_claim(fields: ClusterViewFields, detail: str) -> None:
    """A member claimed its address."""
    fields["holders"][ClusterMemberId.parse(detail).address] = detail


def _apply_release(fields: ClusterViewFields, detail: str) -> None:
    """A member released its address -- only if it still holds it."""
    holders = fields["holders"]
    released_address = ClusterMemberId.parse(detail).address
    if holders.get(released_address) == detail:
        del holders[released_address]


def _apply_mode(fields: ClusterViewFields, detail: str) -> None:
    """The cluster's mode changed."""
    fields["mode"] = detail


def _apply_resize(fields: ClusterViewFields, detail: str) -> None:
    """The cohort was resized."""
    # An address the cohort no longer holds is no one's.
    cohort = _parse_cohort(detail.split())
    fields["cohort"] = cohort
    fields["holders"] = {address: holder for address, holder in fields["holders"].items() if address in cohort}


def _apply_configuration(fields: ClusterViewFields, detail: str) -> None:
    """The Raft configuration's voters and learners changed."""
    configuration = dict(part.split("=", 1) for part in detail.split() if "=" in part)
    fields["voters"] = _parse_members(configuration.get("voters", ""))
    fields["learners"] = _parse_members(configuration.get("learners", ""))


EVENT_APPLIERS: dict[str, Callable[[ClusterViewFields, str], None]] = {
    "claim": _apply_claim,
    "release": _apply_release,
    "mode": _apply_mode,
    "resize": _apply_resize,
    "configuration": _apply_configuration,
}


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
        view = self._folded_view(self._view, reply)
        self._view = view
        self._advance_observed_at(poll_sent_at)
        return view

    def _folded_view(self, view: ClusterView, reply: ClusterWatchReply) -> ClusterView:
        """The view after one reply: a snapshot replaces it, events advance
        it, and an empty reply only re-dates its index (AD-52 section 9)."""
        if reply.snapshot:
            return self._snapshot_view(reply)
        if reply.events:
            return self._advanced_view(view, reply)
        return _redated_view(view, reply)

    def _snapshot_view(self, reply: ClusterWatchReply) -> ClusterView:
        """The view a snapshot reply carries whole, counted as applied."""
        view = ClusterView(
            cluster_uuid=reply.cluster_uuid,
            applied_index=reply.applied_index,
            holders={ClusterMemberId.parse(holder).address: holder for holder in reply.holders},
            cohort=_parse_cohort(reply.cohort),
            voters=frozenset(reply.voters),
            learners=frozenset(reply.learners),
            mode=reply.mode,
        )
        self._snapshots_applied += 1
        return view

    def _advanced_view(self, view: ClusterView, reply: ClusterWatchReply) -> ClusterView:
        """The view advanced by the reply's events in commit order, skipping
        any event at or before the view's index (already in it)."""
        fields: ClusterViewFields = {
            "holders": dict(view.holders),
            "cohort": view.cohort,
            "voters": view.voters,
            "learners": view.learners,
            "mode": view.mode,
        }
        for index, kind, detail in reply.events:
            if index > view.applied_index:
                self._apply_event(fields, kind, detail)
        return ClusterView(
            cluster_uuid=reply.cluster_uuid,
            applied_index=max(view.applied_index, reply.applied_index),
            **fields,
        )

    def _apply_event(self, fields: ClusterViewFields, kind: str, detail: str) -> None:
        """Count one new event and fold it into the view's fields; an
        unknown kind changes nothing."""
        self._events_applied += 1
        if (event_applier := EVENT_APPLIERS.get(kind)) is not None:
            event_applier(fields, detail)

    def _advance_observed_at(self, poll_sent_at: float) -> None:
        """Date the view from the poll that brought it, never backwards."""
        if self._observed_at is None or poll_sent_at > self._observed_at:
            self._observed_at = poll_sent_at

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
