"""
The soft-state cache's watch (AD-52 sections 9-10) when the cluster cannot
be reached -- the real ``ClusterWatchFollower`` and ``ClusterViewCache`` on
virtual time, polling two members that answer, then go silent, then answer
again.

* While a member answers, the view stays fresh and connected.
* Once none answers, the view is still served -- staleness growing -- and
  counts disconnected exactly once its staleness passes what a healthy
  watch ever lets it reach (two polls: each its wait plus the request's
  budget): never before.
* An unreachable cluster costs the follower one pass over its members per
  poll wait -- never a spin.
* The first answer after reconnects it.
* The follower reports each entry into and exit from disconnected mode
  exactly once, as it happens: connected at the first answer, disconnected
  within one poll wait of the view passing the bound, connected again at
  the first answer after.
"""

import asyncio
import contextvars

from hyperscale.distributed.cluster.cluster_view_cache import ClusterViewCache
from hyperscale.distributed.cluster.cluster_watch_follower import ClusterWatchFollower
from hyperscale.distributed.cluster.models.cluster_view import ClusterView
from hyperscale.distributed.cluster.models.cluster_watch_reply import ClusterWatchReply
from hyperscale.distributed.cluster.models.cluster_watch_request import ClusterWatchRequest
from hyperscale.distributed.env import Env
from hyperscale.distributed.runtime import restore_defaults, snapshot_defaults, swap_defaults
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.logging import LoggingConfig
from tests.simulation.harness.sim import SimulationLoop, VirtualClock

SETTINGS = Env()
POLL_WAIT_SECONDS = SETTINGS.CLUSTER_WATCH_WAIT_SECONDS
REQUEST_TIMEOUT_SECONDS = SETTINGS.MANAGER_TCP_TIMEOUT_STANDARD
MEMBERS = (("10.0.0.1", 9000), ("10.0.0.2", 9000))
HOLDERS = ["node-a@10.0.0.1:9000#1", "node-b@10.0.0.2:9000#1"]
# Answering, then unreachable, then answering again -- each phase several
# disconnection bounds long.
# A healthy watch's view is at most two polls old: the one that brought it
# and the next, each up to the wait plus the request's budget.
DISCONNECT_BOUND_SECONDS = 2 * (POLL_WAIT_SECONDS + REQUEST_TIMEOUT_SECONDS)
UNREACHABLE_FROM = 2 * DISCONNECT_BOUND_SECONDS
REACHABLE_FROM = UNREACHABLE_FROM + 4 * DISCONNECT_BOUND_SECONDS
OBSERVE_UNTIL = REACHABLE_FROM + 2 * DISCONNECT_BOUND_SECONDS
SAMPLE_SECONDS = 0.5


def test_an_unreachable_cluster_reads_stale_then_disconnected_and_never_spins() -> None:
    snapshot = snapshot_defaults()
    loop = SimulationLoop()
    clock = VirtualClock(loop)
    swap_defaults(clock=clock)

    async def scenario() -> tuple[
        list[tuple[float, float, bool]], list[float], list[ClusterView], list[tuple[float, float, bool]]
    ]:
        task_runner = TaskRunner()
        polls_sent_at: list[float] = []
        views: list[ClusterView] = []
        transitions: list[tuple[float, float, bool]] = []

        async def send_watch(member: tuple[str, int], payload: bytes, timeout: float) -> bytes | Exception:
            polls_sent_at.append(clock.monotonic())
            request = ClusterWatchRequest.load(payload)
            now = clock.monotonic()
            if UNREACHABLE_FROM <= now < REACHABLE_FROM:
                # Refused at once -- the case a follower without its pause
                # would spin on (a real refusal still yields to the loop
                # once, at the same instant).
                await asyncio.sleep(0)
                return ConnectionRefusedError(f"{member} unreachable")
            if request.cluster_uuid is None:
                return ClusterWatchReply(
                    served=True,
                    cluster_uuid="cluster-1",
                    applied_index=10,
                    snapshot=True,
                    holders=HOLDERS,
                    mode="open",
                    cohort=[f"{host}:{port}" for host, port in MEMBERS],
                ).dump()
            # Nothing changes: the member holds the poll its whole wait.
            await clock.sleep(request.wait_seconds)
            return ClusterWatchReply(served=True, cluster_uuid="cluster-1", applied_index=10).dump()

        cache = ClusterViewCache(POLL_WAIT_SECONDS, REQUEST_TIMEOUT_SECONDS)

        async def record_transition(disconnected: bool) -> None:
            now = clock.monotonic()
            transitions.append((now, cache.read(now)[1], disconnected))
        follower = ClusterWatchFollower(
            cache,
            seeds=lambda: MEMBERS,
            send_watch=send_watch,
            clock=clock,
            poll_wait_seconds=POLL_WAIT_SECONDS,
            request_timeout_seconds=REQUEST_TIMEOUT_SECONDS,
            on_view_changed=views.append,
            on_disconnected_changed=record_transition,
        )
        task_runner.run(follower.run, alias="watch")
        samples: list[tuple[float, float, bool]] = []
        while clock.monotonic() < OBSERVE_UNTIL:
            now = clock.monotonic()
            _view, staleness = cache.read(now)
            samples.append((now, staleness, cache.is_disconnected(now)))
            await clock.sleep(SAMPLE_SECONDS)
        follower.stop()
        await task_runner.shutdown()
        return samples, polls_sent_at, views, transitions

    def run():
        LoggingConfig().disable()
        task = loop.create_task(scenario())
        loop.run_window(OBSERVE_UNTIL + 2 * DISCONNECT_BOUND_SECONDS)
        assert task.done()
        return task.result()

    try:
        samples, polls_sent_at, views, transitions = contextvars.copy_context().run(run)
    finally:
        loop.close()
        restore_defaults(snapshot)

    # One view: the snapshot -- an unchanged cluster brings no other.
    assert len(views) == 1 and views[0].holders and views[0].mode == "open", views

    connected_phase = [sample for sample in samples if 0 < sample[0] < UNREACHABLE_FROM]
    assert connected_phase and not any(disconnected for _now, _staleness, disconnected in connected_phase), (
        connected_phase
    )
    # While reachable, never staler than a healthy watch lets it get.
    assert all(staleness <= DISCONNECT_BOUND_SECONDS for _now, staleness, _d in connected_phase), connected_phase

    unreachable_phase = [sample for sample in samples if UNREACHABLE_FROM <= sample[0] < REACHABLE_FROM]
    # Disconnected exactly when its staleness passed the bound -- never
    # before, always after.
    assert all(
        disconnected == (staleness > DISCONNECT_BOUND_SECONDS)
        for _now, staleness, disconnected in unreachable_phase
    ), unreachable_phase
    assert any(disconnected for _now, _staleness, disconnected in unreachable_phase), unreachable_phase

    # No spin: each unreachable pass over the members is followed by a
    # poll's wait -- at most one pass (two polls) per wait, plus the first.
    unreachable_polls = [sent_at for sent_at in polls_sent_at if UNREACHABLE_FROM <= sent_at < REACHABLE_FROM]
    unreachable_seconds = REACHABLE_FROM - UNREACHABLE_FROM
    assert len(unreachable_polls) <= len(MEMBERS) * (unreachable_seconds / POLL_WAIT_SECONDS + 1), (
        unreachable_polls
    )

    # Reconnected by the first answer after the cluster came back.
    reconnected = [sample for sample in samples if sample[0] >= REACHABLE_FROM + DISCONNECT_BOUND_SECONDS]
    assert reconnected and not any(disconnected for _now, _staleness, disconnected in reconnected), reconnected

    # Each transition reported once, as it happened.
    assert [disconnected for _now, _staleness, disconnected in transitions] == [False, True, False], transitions
    (connected_at, _s, _d), (disconnected_at, staleness_at_loss, _d2), (reconnected_at, _s2, _d3) = transitions
    assert connected_at < UNREACHABLE_FROM, transitions
    # Lost only past the bound, and noticed within the poll wait that
    # separates two passes over the members.
    assert UNREACHABLE_FROM <= disconnected_at < REACHABLE_FROM, transitions
    assert DISCONNECT_BOUND_SECONDS < staleness_at_loss <= DISCONNECT_BOUND_SECONDS + POLL_WAIT_SECONDS, transitions
    # Back at the first answer: the poll sent within a wait of the cluster
    # returning, answered after its own wait.
    assert REACHABLE_FROM <= reconnected_at <= REACHABLE_FROM + 2 * POLL_WAIT_SECONDS, transitions
