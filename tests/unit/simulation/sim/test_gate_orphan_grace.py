"""
A gate waits on a job whose leader gate failed for as long as the
gate tier needs to reach its verdict on that gate -- derived, learned from
rescues, and extended while the leader is still heard from -- never for a
fixed guess (AD-52 section 10 for gates, as the worker's orphan grace).

The real ``GateOrphanJobCoordinator`` and ``GateRuntimeState`` on a
``SimulationLoop``, configured from ``Env`` as the gate server configures
them. The moment the coordinator decides an orphan is due is observed
through its one question to the gate -- whether this gate leads the tier,
asked only of a job it is about to take over -- answered "no", so the
takeover itself stays out of scope.

* A silent leader's job is due once the derived grace has passed -- not
  before -- within one check interval.
* A leader still heard from earns decaying AD-26 extensions, past the
  grace, and its job is still due once they run out.
* A rescue that took longer than the grace raises it for the next orphan.
"""

import contextvars
from typing import Any, Callable, Coroutine, TypeVar

from hyperscale.distributed.env import Env
from hyperscale.distributed.jobs import JobLeadershipTracker
from hyperscale.distributed.jobs.gates import GateJobManager
from hyperscale.distributed.jobs.gates.consistent_hash_ring import ConsistentHashRing
from hyperscale.distributed.models import GateInfo, GlobalJobStatus, JobStatus
from hyperscale.distributed.nodes.gate.config import derive_gate_orphan_grace_seconds
from hyperscale.distributed.nodes.gate.orphan_job_coordinator import GateOrphanJobCoordinator
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.runtime import restore_defaults, snapshot_defaults, swap_defaults
from hyperscale.distributed.swim.core import NodeId
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.logging import Logger, LoggingConfig
from tests.simulation.harness.sim import SimulationLoop, VirtualClock

ScenarioResult = TypeVar("ScenarioResult")

SETTINGS = Env()
GRACE_SECONDS = derive_gate_orphan_grace_seconds(SETTINGS)
CHECK_INTERVAL_SECONDS = SETTINGS.GATE_ORPHAN_CHECK_INTERVAL
THIS_GATE_ADDRESS = ("10.0.0.1", 9000)
LEADER_GATE_ID = "gate-leader"
LEADER_GATE_ADDRESS = ("10.0.0.2", 9000)
# When the leader gate fails, well after boot.
ORPHANED_AT = 10.0
# How often a leader still alive is heard from: one gate heartbeat per
# check interval is ample evidence; the extension needs only one since the
# last grant.
HEARTBEAT_EVERY_SECONDS = CHECK_INTERVAL_SECONDS / 2
# How much later than a window's end its AD-26 extensions can carry a
# decision: grants halve from the window itself (``base / 2**count``, the
# count from 0), so together they stay under two more windows, each grant
# landing on a check.
def extensions_bound_seconds(window_seconds: float) -> float:
    return 2 * window_seconds + (SETTINGS.EXTENSION_MAX_EXTENSIONS + 1) * CHECK_INTERVAL_SECONDS


def simulate(
    scenario: Callable[[VirtualClock], Coroutine[Any, Any, ScenarioResult]],
    until: float,
) -> ScenarioResult:
    """Run ``scenario`` on a fresh ``SimulationLoop`` through virtual
    ``until``, every clock on its virtual time and logging off."""
    snapshot = snapshot_defaults()
    loop = SimulationLoop()
    clock = VirtualClock(loop)
    swap_defaults(clock=clock)

    def run_through_deadline() -> ScenarioResult:
        LoggingConfig().disable()
        scenario_task = loop.create_task(scenario(clock))
        loop.run_window(until)
        assert scenario_task.done(), f"the scenario was still running at virtual {until}"
        return scenario_task.result()

    try:
        return contextvars.copy_context().run(run_through_deadline)
    finally:
        loop.close()
        restore_defaults(snapshot)


async def run_orphans(
    clock: VirtualClock,
    orphan_job_ids: list[str],
    rescues: dict[str, float],
    heard_from_during: set[str],
    observe_seconds: float,
    await_failure: bool = False,
) -> tuple[dict[str, float], dict[str, float]]:
    """Orphan each of ``orphan_job_ids`` in turn -- the next once the one
    before is settled -- by its leader gate's failure (SWIM); the leader (and so
    the tier) is heard from while a job of ``heard_from_during`` waits.
    Rescue a job (its leadership announced by a peer) ``rescues[job_id]``
    seconds after it came due. No gate ever takes a job over: each is
    settled once due, or with ``await_failure`` once the coordinator fails
    it. Returns when each job first came due, and when each failed,
    relative to its orphaning."""
    task_runner = TaskRunner()
    state = GateRuntimeState(forward_throughput_interval_start=clock.monotonic())
    state.add_known_gate(
        LEADER_GATE_ID,
        GateInfo(
            node_id=LEADER_GATE_ID,
            tcp_host=LEADER_GATE_ADDRESS[0],
            tcp_port=LEADER_GATE_ADDRESS[1],
            udp_host=LEADER_GATE_ADDRESS[0],
            udp_port=LEADER_GATE_ADDRESS[1] + 1,
            datacenter="dc-1",
        ),
    )
    job_manager = GateJobManager()
    node_id = NodeId.generate(datacenter="dc-1", priority=50, host=THIS_GATE_ADDRESS[0], port=THIS_GATE_ADDRESS[1])
    orphaned_at: dict[str, float] = {}
    due_after: dict[str, float] = {}
    failed_after: dict[str, float] = {}
    current_job: list[str] = []

    def asked_whether_leading() -> bool:
        (job_id,) = current_job
        due_after.setdefault(job_id, clock.monotonic() - orphaned_at[job_id])
        return False

    async def no_send(address: tuple[str, int], action: str, payload: bytes, timeout: float) -> bytes:
        raise AssertionError(f"the coordinator sent {action} to {address}")

    async def record_failure(job_id: str, datacenters: tuple[str, ...], reason: str) -> None:
        assert await_failure, f"the coordinator failed {job_id}: {reason}"
        failed_after[job_id] = clock.monotonic() - orphaned_at[job_id]

    job_leadership_tracker = JobLeadershipTracker(node_id=node_id.full, node_addr=THIS_GATE_ADDRESS)
    coordinator = GateOrphanJobCoordinator(
        state=state,
        logger=Logger(),
        task_runner=task_runner,
        job_hash_ring=ConsistentHashRing(),
        job_leadership_tracker=job_leadership_tracker,
        job_manager=job_manager,
        get_node_id=lambda: node_id,
        get_node_addr=lambda: THIS_GATE_ADDRESS,
        send_tcp=no_send,
        get_active_peers=lambda: set(),
        clock=clock,
        is_cluster_leader=asked_whether_leading,
        orphan_check_interval_seconds=CHECK_INTERVAL_SECONDS,
        orphan_grace_period_seconds=GRACE_SECONDS,
        orphan_extension_min_grant_seconds=SETTINGS.EXTENSION_MIN_GRANT,
        orphan_extension_max_extensions=SETTINGS.EXTENSION_MAX_EXTENSIONS,
        finalize_failed_job=record_failure,
    )

    async def hear_from_leader() -> None:
        while True:
            if current_job and current_job[0] in heard_from_during:
                state.record_gate_peer_heartbeat(LEADER_GATE_ADDRESS)
            await clock.sleep(HEARTBEAT_EVERY_SECONDS)

    task_runner.run(hear_from_leader)
    await coordinator.start()
    try:
        await clock.sleep(ORPHANED_AT)
        for job_id in orphan_job_ids:
            job_manager.set_job(job_id, GlobalJobStatus(job_id=job_id, status=JobStatus.RUNNING.value))
            current_job[:] = [job_id]
            orphaned_at[job_id] = clock.monotonic()
            job_leadership_tracker.process_leadership_claim(job_id, LEADER_GATE_ID, LEADER_GATE_ADDRESS, 1)
            assert coordinator.mark_jobs_orphaned_by_gate(LEADER_GATE_ADDRESS) == [job_id]
            if (rescued_after := rescues.get(job_id)) is not None:
                while job_id not in due_after:
                    await clock.sleep(CHECK_INTERVAL_SECONDS)
                await clock.sleep(orphaned_at[job_id] + due_after[job_id] + rescued_after - clock.monotonic())
                coordinator.clear_orphaned_job(job_id)
                job_leadership_tracker.release_leadership(job_id)
                continue
            settled = failed_after if await_failure else due_after
            while job_id not in settled and clock.monotonic() - orphaned_at[job_id] < observe_seconds:
                await clock.sleep(CHECK_INTERVAL_SECONDS)
            job_manager.delete_job(job_id)
            job_leadership_tracker.release_leadership(job_id)
            if not await_failure:
                coordinator.clear_orphaned_job(job_id)
        return due_after, failed_after
    finally:
        await coordinator.stop()
        await task_runner.shutdown()


def test_a_silent_leaders_job_is_due_once_the_derived_grace_has_passed() -> None:
    async def scenario(clock: VirtualClock) -> tuple[dict[str, float], dict[str, float]]:
        return await run_orphans(clock, ["job-silent"], {}, heard_from_during=set(), observe_seconds=4 * GRACE_SECONDS)

    due_after, _ = simulate(scenario, ORPHANED_AT + 5 * GRACE_SECONDS)

    assert GRACE_SECONDS <= due_after["job-silent"] < GRACE_SECONDS + CHECK_INTERVAL_SECONDS, due_after


def test_a_leader_still_heard_from_is_extended_and_its_job_still_comes_due() -> None:
    """AD-26 grants halve from the grace itself (``base / 2**count``, the
    count from 0): the first alone doubles a silent leader's wait, and
    together they stay under two more graces."""

    async def scenario(clock: VirtualClock) -> tuple[dict[str, float], dict[str, float]]:
        return await run_orphans(clock, ["job-heard"], {}, heard_from_during={"job-heard"}, observe_seconds=4 * GRACE_SECONDS)

    due_after, _ = simulate(scenario, ORPHANED_AT + 5 * GRACE_SECONDS)

    assert "job-heard" in due_after, "a leader heard from forever held its job forever"
    assert due_after["job-heard"] >= 2 * GRACE_SECONDS, due_after
    assert due_after["job-heard"] < 3 * GRACE_SECONDS + (SETTINGS.EXTENSION_MAX_EXTENSIONS + 1) * CHECK_INTERVAL_SECONDS, (
        due_after
    )


def test_a_rescue_slower_than_the_grace_raises_it_for_the_next_orphan() -> None:
    rescued_after_due_seconds = 4 * CHECK_INTERVAL_SECONDS

    async def scenario(clock: VirtualClock) -> tuple[dict[str, float], dict[str, float]]:
        return await run_orphans(
            clock,
            ["job-rescued", "job-after"],
            {"job-rescued": rescued_after_due_seconds},
            heard_from_during=set(),
            observe_seconds=4 * GRACE_SECONDS,
        )

    due_after, _ = simulate(scenario, ORPHANED_AT + 6 * GRACE_SECONDS)

    # The rescued job came due at the grace (its takeover was deferred to the
    # tier's leader), and its rescue landed later still...
    assert GRACE_SECONDS <= due_after["job-rescued"] < GRACE_SECONDS + CHECK_INTERVAL_SECONDS, due_after
    slow_rescue_seconds = due_after["job-rescued"] + rescued_after_due_seconds
    # ...so the next orphan waits as long as that rescue took.
    assert slow_rescue_seconds <= due_after["job-after"] < slow_rescue_seconds + CHECK_INTERVAL_SECONDS, due_after


def test_a_due_job_its_tier_never_takes_over_fails_one_failover_later() -> None:
    """Due at the grace, the tier gets one more failover to produce a
    leader that can take it over; a silent tier does not, and the job
    fails -- not 300 seconds of guessing later."""

    async def scenario(clock: VirtualClock) -> tuple[dict[str, float], dict[str, float]]:
        return await run_orphans(
            clock, ["job-stranded"], {}, heard_from_during=set(), observe_seconds=6 * GRACE_SECONDS, await_failure=True
        )

    due_after, failed_after = simulate(scenario, ORPHANED_AT + 7 * GRACE_SECONDS)

    came_due = due_after["job-stranded"]
    assert GRACE_SECONDS <= came_due < GRACE_SECONDS + CHECK_INTERVAL_SECONDS, due_after
    assert came_due + GRACE_SECONDS <= failed_after["job-stranded"] < came_due + GRACE_SECONDS + CHECK_INTERVAL_SECONDS, (
        due_after,
        failed_after,
    )


def test_a_tier_still_heard_from_extends_a_due_jobs_takeover_window() -> None:
    """Peer gates still heartbeating may yet elect the leader the job waits
    on: its takeover window extends, decaying, and it still fails once the
    extensions run out."""

    async def scenario(clock: VirtualClock) -> tuple[dict[str, float], dict[str, float]]:
        return await run_orphans(
            clock, ["job-waiting"], {}, heard_from_during={"job-waiting"}, observe_seconds=8 * GRACE_SECONDS, await_failure=True
        )

    due_after, failed_after = simulate(scenario, ORPHANED_AT + 9 * GRACE_SECONDS)

    came_due = due_after["job-waiting"]
    assert "job-waiting" in failed_after, "a tier heard from forever held its job forever"
    assert failed_after["job-waiting"] >= came_due + 2 * GRACE_SECONDS, (due_after, failed_after)
    assert failed_after["job-waiting"] < came_due + GRACE_SECONDS + extensions_bound_seconds(GRACE_SECONDS), (
        due_after,
        failed_after,
    )


def test_a_takeover_slower_than_the_window_raises_it_for_the_next_due_job() -> None:
    """A takeover can only outlast the window while extensions hold the job
    -- its tier still heard from. One landing that late raises the window:
    the next due job, its tier silent, fails only a full learned window
    after it came due."""
    rescued_after_due_seconds = GRACE_SECONDS + 4 * CHECK_INTERVAL_SECONDS

    async def scenario(clock: VirtualClock) -> tuple[dict[str, float], dict[str, float]]:
        return await run_orphans(
            clock,
            ["job-slow", "job-after"],
            {"job-slow": rescued_after_due_seconds},
            heard_from_during={"job-slow"},
            observe_seconds=20 * GRACE_SECONDS,
            await_failure=True,
        )

    due_after, failed_after = simulate(scenario, ORPHANED_AT + 30 * GRACE_SECONDS)

    assert "job-slow" not in failed_after, failed_after
    came_due = due_after["job-after"]
    assert (
        came_due + rescued_after_due_seconds
        <= failed_after["job-after"]
        < came_due + rescued_after_due_seconds + CHECK_INTERVAL_SECONDS
    ), (due_after, failed_after)
