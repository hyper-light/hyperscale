"""
Keeps a ``ClusterViewCache`` current by watching a cluster (AD-52 sections
9-10): the data plane reads the cache, never the control plane.
"""

from collections.abc import Awaitable, Callable, Iterable

from hyperscale.distributed.runtime import Clock

from .cluster_view_cache import ClusterViewCache
from .models.cluster_view import ClusterView
from .models.cluster_watch_reply import ClusterWatchReply
from .models.cluster_watch_request import ClusterWatchRequest

MemberAddress = tuple[str, int]
# Sends a watch poll to a member with the poll's own timeout; the reply's
# bytes, or the failure.
SendWatch = Callable[[MemberAddress, bytes, float], Awaitable[bytes | Exception | None]]


def _loaded_reply(response: bytes | Exception | None) -> ClusterWatchReply | None:
    """The reply a poll's response carries; None for a failure or nothing."""
    return None if isinstance(response, Exception) or not response else ClusterWatchReply.load(response)


def _served_reply(response: bytes | Exception | None) -> ClusterWatchReply | None:
    """The reply a member served; None when the poll failed or the member
    did not serve it."""
    reply = _loaded_reply(response)
    return reply if reply is not None and reply.served else None


class ClusterWatchFollower:
    """Long-polls one member of a cluster at a time for its membership
    changes and folds them into a ``ClusterViewCache``. A member that does
    not answer is passed over for the next -- every member the view holds,
    and the seeds it started from, in a fixed order -- and once none
    answers, the follower waits one poll's wait before going round again:
    an unreachable cluster costs the time a healthy poll would have, never
    a spin. ``on_view_changed`` hears every view that differs from the one
    before it; ``on_disconnected_changed`` hears each time the cache enters
    or leaves disconnected mode (AD-52 section 10) -- True when its view
    grew staler than a healthy watch lets it, False when an answer
    refreshed it (the first answer included)."""

    __slots__ = (
        "_cache",
        "_seeds",
        "_send_watch",
        "_clock",
        "_poll_wait_seconds",
        "_poll_timeout_seconds",
        "_on_view_changed",
        "_on_disconnected_changed",
        "_disconnected",
        "_target_index",
        "_running",
        "_polls_answered",
        "_polls_failed",
    )

    def __init__(
        self,
        cache: ClusterViewCache,
        seeds: Callable[[], Iterable[MemberAddress]],
        send_watch: SendWatch,
        clock: Clock,
        poll_wait_seconds: float,
        request_timeout_seconds: float,
        on_view_changed: Callable[[ClusterView], None],
        on_disconnected_changed: Callable[[bool], Awaitable[None]],
    ) -> None:
        """
        Args:
            cache: The cache the watch keeps current
            seeds: The members this node knows of besides those the view
                holds -- read each round, so ones it learns later count
            send_watch: Sends a poll to a member with a timeout
            clock: The runtime clock
            poll_wait_seconds: How long a member holds a poll for a change
            request_timeout_seconds: The request's budget beyond the wait
            on_view_changed: Hears each new view
            on_disconnected_changed: Hears each entry into (True) and exit
                from (False) disconnected mode
        """
        self._cache = cache
        self._seeds = seeds
        self._send_watch = send_watch
        self._clock = clock
        self._poll_wait_seconds = poll_wait_seconds
        self._poll_timeout_seconds = poll_wait_seconds + request_timeout_seconds
        self._on_view_changed = on_view_changed
        self._on_disconnected_changed = on_disconnected_changed
        # A cache with no observation yet is disconnected.
        self._disconnected = True
        self._target_index = 0
        self._running = False
        self._polls_answered = 0
        self._polls_failed = 0

    async def run(self) -> None:
        """Follow the cluster until ``stop``."""
        self._running = True
        failures_in_a_row = 0
        while self._running:
            failures_in_a_row = await self._poll_once(failures_in_a_row)

    async def _poll_once(self, failures_in_a_row: int) -> int:
        """Poll the next member once and fold in its answer (AD-52 section
        9); returns the failures in a row after this poll."""
        view = self._cache.view
        targets = sorted({*self._seeds(), *view.holders.keys()})
        if not targets:
            await self._clock.sleep(self._poll_wait_seconds)
            return failures_in_a_row
        target = targets[self._target_index % len(targets)]
        sent_at = self._clock.monotonic()
        response = await self._send_watch(
            target,
            ClusterWatchRequest(
                cluster_uuid=view.cluster_uuid,
                after_index=view.applied_index,
                wait_seconds=self._poll_wait_seconds,
            ).dump(),
            self._poll_timeout_seconds,
        )
        if (reply := _served_reply(response)) is None:
            return await self._record_failure(failures_in_a_row, len(targets))
        await self._record_answer(reply, sent_at, view)
        return 0

    async def _record_failure(self, failures_in_a_row: int, target_count: int) -> int:
        """Pass over a member that did not answer; once none of the round
        answered, wait one poll's wait (never a spin). Returns the failures
        in a row."""
        self._polls_failed += 1
        self._target_index += 1
        failures_in_a_row += 1
        await self._refresh_disconnected()
        if failures_in_a_row >= target_count:
            await self._clock.sleep(self._poll_wait_seconds)
            return 0
        return failures_in_a_row

    async def _record_answer(self, reply: ClusterWatchReply, sent_at: float, view: ClusterView) -> None:
        """Fold an answer into the cache and tell whoever listens of a new
        view and of a disconnected-mode change (AD-52 section 10)."""
        self._polls_answered += 1
        if (applied := self._cache.apply(reply, sent_at)) != view:
            self._on_view_changed(applied)
        await self._refresh_disconnected()

    async def _refresh_disconnected(self) -> None:
        """Announce each entry into or exit from disconnected mode."""
        if (disconnected := self._cache.is_disconnected(self._clock.monotonic())) != self._disconnected:
            self._disconnected = disconnected
            await self._on_disconnected_changed(disconnected)

    def stop(self) -> None:
        self._running = False

    def get_stats(self) -> dict[str, int]:
        return {
            "polls_answered": self._polls_answered,
            "polls_failed": self._polls_failed,
            "disconnected": int(self._disconnected),
        }
