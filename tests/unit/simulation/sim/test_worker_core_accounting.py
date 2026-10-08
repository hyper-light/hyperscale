"""
The manager's view of a worker's free cores (WorkerPool) against the
worker's own (CoreAllocator), across reordered reports.

Every report of a worker's free cores -- heartbeat, progress, result --
used to wipe all of the manager's reservations. A report computed before
the worker took a dispatch still in flight erased that dispatch's
reservation, so the manager booked the same cores twice; a report that
already showed an accepted dispatch, followed by its ack, subtracted those
cores twice. Now each report and each dispatch ack carries the worker
allocator's availability version, and a reservation stands exactly until
the free count the manager applied reflects its dispatch.

Driven VOPR-style: a real pool and a real allocator; seeded schedules of
allocations, dispatch deliveries (taken or refused), acks, partial and
whole frees, and heartbeats / progress reports / results delivered in
any order. Checked after every step: the manager never believes more
cores free than the worker truly has, less what is still in flight to it
(safety -- the double booking). Checked once everything is delivered: its
view is exactly the worker's (no double subtraction).
"""

import asyncio
import random

import pytest

from hyperscale.distributed.jobs.core_allocator import CoreAllocator
from hyperscale.distributed.jobs.worker_pool import WorkerPool
from hyperscale.distributed.models import NodeInfo, WorkerHeartbeat, WorkerRegistration, WorkerState

WORKER = "worker-1"
JOB = "job-1"
TOTAL_CORES = 8
SEEDS = range(40)
STEPS = 300


def registration() -> WorkerRegistration:
    return WorkerRegistration(
        node=NodeInfo(node_id=WORKER, role="worker", host="127.0.0.1", port=10_001, datacenter="dc", udp_port=10_002),
        total_cores=TOTAL_CORES,
        available_cores=TOTAL_CORES,
        memory_mb=1024,
    )


class SimulatedWorker:
    """The worker's side: a real allocator, its running dispatches, and the
    reports it sends (each a snapshot taken with no await in between)."""

    def __init__(self) -> None:
        self.allocator = CoreAllocator(TOTAL_CORES, WORKER)
        self.running: dict[str, int] = {}
        self.state_version = 0

    def heartbeat(self) -> WorkerHeartbeat:
        self.state_version += 1
        return WorkerHeartbeat(
            node_id=WORKER,
            state=WorkerState.HEALTHY.value,
            available_cores=self.allocator.available_cores,
            cores_version=self.allocator.availability_version,
            queue_depth=0,
            cpu_percent=0.0,
            memory_percent=0.0,
            version=self.state_version,
            total_cores=TOTAL_CORES,
            active_workflows={token: "running" for token in self.running},
        )

    def core_report(self, token: str) -> tuple[str, int, int]:
        return (token, self.allocator.available_cores, self.allocator.availability_version)


def manager_free_cores(pool: WorkerPool) -> int:
    worker = pool._workers[WORKER]
    return worker.available_cores - worker.reserved_cores


@pytest.mark.parametrize("seed", SEEDS)
def test_the_manager_never_books_cores_the_worker_does_not_have(seed: int) -> None:
    asyncio.run(run_schedule(seed))


async def run_schedule(seed: int) -> None:
    draw = random.Random(seed)
    pool = WorkerPool()
    await pool.register_worker(registration())
    worker = SimulatedWorker()
    in_flight_dispatches: dict[str, int] = {}  # sent by the manager, not yet at the worker
    pending_acks: list[tuple[str, bool, int]] = []  # (token, taken, allocated at version)
    pending_reports: list[tuple[str, object]] = []  # delivered in any order
    dispatch_count = 0

    async def deliver_report(kind: str, report: object) -> None:
        if kind == "heartbeat":
            await pool.process_heartbeat(WORKER, report)
        else:
            token, available, version = report
            await pool.update_worker_cores_from_progress(WORKER, available, token, version)

    for _step in range(STEPS):
        action = draw.random()
        if action < 0.2:
            cores_wanted = draw.randint(1, 3)
            token = f"{JOB}:dispatch-{dispatch_count}"
            dispatch_count += 1
            allocations = await pool.allocate_cores(
                cores_wanted, job_id=JOB, dispatch_token_for=lambda _worker_id, token=token: token
            )
            if allocations:
                ((_worker_id, cores),) = allocations
                in_flight_dispatches[token] = cores
        elif action < 0.35 and in_flight_dispatches:
            # A dispatch reaches the worker: taken if its cores are free.
            token = draw.choice(sorted(in_flight_dispatches))
            cores = in_flight_dispatches.pop(token)
            result = await worker.allocator.allocate(token, cores)
            if result.success:
                worker.running[token] = cores
            pending_acks.append((token, result.success, result.availability_version))
        elif action < 0.45 and pending_acks:
            token, taken, allocated_at_version = pending_acks.pop(draw.randrange(len(pending_acks)))
            if taken:
                await pool.record_dispatch_taken(WORKER, token, allocated_at_version)
            else:
                await pool.release_cores(WORKER, token)
        elif action < 0.55 and worker.running:
            # Part of a running dispatch's cores finish (a progress report).
            token = draw.choice(sorted(worker.running))
            if worker.running[token] > 1:
                await worker.allocator.free_subset(token, 1)
                worker.running[token] -= 1
            pending_reports.append(("progress", worker.core_report(token)))
        elif action < 0.65 and worker.running:
            # A dispatch finishes. Its result reaches this manager -- or
            # goes to the job's new leader instead, and never arrives here.
            token = draw.choice(sorted(worker.running))
            await worker.allocator.free(token)
            del worker.running[token]
            if draw.random() < 0.7:
                pending_reports.append(("result", worker.core_report(token)))
        elif action < 0.8:
            pending_reports.append(("heartbeat", worker.heartbeat()))
        elif pending_reports:
            kind, report = pending_reports.pop(draw.randrange(len(pending_reports)))
            await deliver_report(kind, report)

        # Safety: never more free here than the worker has, less what is on
        # its way to it.
        assert manager_free_cores(pool) <= worker.allocator.available_cores - sum(in_flight_dispatches.values()), (
            seed,
            _step,
        )

    # Quiesce: every dispatch delivered, every report applied, a last
    # heartbeat -- and only then the acks: an ack whose dispatch the applied
    # count already reflects (it finished, its result lost to another
    # manager) clears its reservation at once. The view is exact.
    for token in sorted(in_flight_dispatches):
        cores = in_flight_dispatches.pop(token)
        result = await worker.allocator.allocate(token, cores)
        if result.success:
            worker.running[token] = cores
        pending_acks.append((token, result.success, result.availability_version))
    for kind, report in pending_reports:
        await deliver_report(kind, report)
    await deliver_report("heartbeat", worker.heartbeat())
    for token, taken, allocated_at_version in pending_acks:
        if taken:
            await pool.record_dispatch_taken(WORKER, token, allocated_at_version)
        else:
            await pool.release_cores(WORKER, token)

    assert manager_free_cores(pool) == worker.allocator.available_cores
    assert pool._workers[WORKER].reserved_cores == 0


def test_an_ack_the_applied_count_already_reflects_clears_its_reservation_at_once() -> None:
    """The worker takes a dispatch and finishes it between heartbeats; its
    result goes to another manager. The next heartbeat (no longer listing
    it) is applied here before the dispatch's ack arrives. The ack shows the
    applied count already reflects the dispatch: its reservation clears then
    -- it does not hold the cores until some later report."""

    async def scenario() -> None:
        pool = WorkerPool()
        await pool.register_worker(registration())
        worker = SimulatedWorker()
        token = f"{JOB}:dispatch-0"
        await pool.allocate_cores(2, job_id=JOB, dispatch_token_for=lambda _worker_id: token)

        allocation = await worker.allocator.allocate(token, 2)
        await worker.allocator.free(token)
        await pool.process_heartbeat(WORKER, worker.heartbeat())
        assert manager_free_cores(pool) == TOTAL_CORES - 2  # the dispatch is unaccounted for yet

        await pool.record_dispatch_taken(WORKER, token, allocation.availability_version)

        assert manager_free_cores(pool) == TOTAL_CORES
        assert pool._workers[WORKER].reserved_cores == 0

    asyncio.run(scenario())


def test_a_finished_jobs_unaccounted_reservations_end_with_it() -> None:
    """Dispatches no report here ever accounted for (their worker's reports
    went to the job's new leader) are released when the job ends here --
    another job's stand."""

    async def scenario() -> None:
        pool = WorkerPool()
        await pool.register_worker(registration())
        await pool.allocate_cores(2, job_id=JOB, dispatch_token_for=lambda _worker_id: f"{JOB}:dispatch-0")
        await pool.allocate_cores(3, job_id="job-2", dispatch_token_for=lambda _worker_id: "job-2:dispatch-0")

        assert await pool.release_job_reservations(JOB) == 1

        assert pool._workers[WORKER].reserved_cores == 3
        assert manager_free_cores(pool) == TOTAL_CORES - 3

    asyncio.run(scenario())
