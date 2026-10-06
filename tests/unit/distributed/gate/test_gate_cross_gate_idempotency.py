"""
AD-40 across gates: an idempotency key decides one job, at every gate.

Idempotency state never left the gate that admitted a submission: when it
died after acknowledging, the client's retry under the same key at another
gate found no entry and admitted the job again -- it ran twice. Two gates
admitting the key at once each committed a job of their own. Now:

* a job's replica carries its submission's key, and every gate that
  commits the replica records the key as decided for that job -- a retry
  there is answered for the original job, marked a duplicate;
* no gate prepares a replica of another job under a key a prepared or
  committed replica holds -- quorums intersect, so of two gates admitting
  the key at once, at most one commits;
* a gate's own pending submission of the job is left for its commit to
  record (the full answer), and a decision already held is never replaced.

Driven through real ``GateJobReplicationCoordinator`` instances exchanging
the 2PC in memory, the real ``GateIdempotencyCache``, and the real gate job
handler.
"""

import asyncio
import dataclasses

import pytest

from hyperscale.distributed.idempotency.gate_cache import GateIdempotencyCache
from hyperscale.distributed.idempotency.idempotency_config import IdempotencyConfig
from hyperscale.distributed.idempotency.idempotency_key import IdempotencyKey
from hyperscale.distributed.idempotency.idempotency_status import IdempotencyStatus
from hyperscale.distributed.models import JobAck, JobSubmission
from tests.unit.distributed.gate.test_gate_job_handler import WORKFLOWS, create_mock_handler
from tests.unit.distributed.gate.test_gate_replica_versions import QUORUM, GateTier, accepted_replica

KEY = IdempotencyKey(client_id="client-1", sequence=7, nonce="nonce-7")


def replica_of(job_id: str, leader_id: str, idempotency_key: str = str(KEY)):
    return dataclasses.replace(accepted_replica(leader_id), job_id=job_id, idempotency_key=idempotency_key)


@pytest.mark.asyncio
@pytest.mark.parametrize("first_gate", ["gate-a", "gate-c"])
async def test_two_gates_admitting_one_key_at_once_commit_at_most_one_job(first_gate: str) -> None:
    tier = GateTier()
    second_gate = "gate-c" if first_gate == "gate-a" else "gate-a"

    committed = await asyncio.gather(
        tier.coordinators[first_gate].replicate_with_quorum(
            replica_of("job-from-first", first_gate), tier.peers_of(first_gate), QUORUM
        ),
        tier.coordinators[second_gate].replicate_with_quorum(
            replica_of("job-from-second", second_gate), tier.peers_of(second_gate), QUORUM
        ),
    )

    assert sum(committed) <= 1, committed


@pytest.mark.asyncio
async def test_a_key_decided_for_one_job_refuses_another_and_keeps_its_own_revisions() -> None:
    tier = GateTier()
    assert await tier.coordinators["gate-a"].replicate_with_quorum(
        replica_of("job-original", "gate-a"), tier.peers_of("gate-a"), QUORUM
    )

    # Another job under the key -- at any gate -- is refused.
    assert not await tier.coordinators["gate-b"].replicate_with_quorum(
        replica_of("job-retried-elsewhere", "gate-b"), tier.peers_of("gate-b"), QUORUM
    )
    # The job's own revisions keep its key.
    assert await tier.coordinators["gate-a"].revise_committed_replica(
        "job-original",
        lambda committed: dataclasses.replace(committed, status_seed="running"),
        tier.peers_of("gate-a"),
        QUORUM,
    )
    # A job with no key is not checked at all.
    assert await tier.coordinators["gate-b"].replicate_with_quorum(
        replica_of("job-without-key", "gate-b", idempotency_key=""), tier.peers_of("gate-b"), QUORUM
    )


def make_cache() -> GateIdempotencyCache[bytes]:
    return GateIdempotencyCache(IdempotencyConfig(), task_runner=None, logger=None)


@pytest.mark.asyncio
async def test_adoption_records_the_decision_without_replacing_one_held() -> None:
    cache = make_cache()
    await cache.adopt_committed(KEY, "job-original", "gate-a")
    adopted = await cache.get(KEY)
    assert (adopted.status, adopted.job_id) == (IdempotencyStatus.COMMITTED, "job-original")

    # A decision held is never replaced.
    await cache.adopt_committed(KEY, "job-other", "gate-c")
    assert (await cache.get(KEY)).job_id == "job-original"


@pytest.mark.asyncio
async def test_adoption_answers_another_jobs_pending_submission_and_leaves_its_own() -> None:
    cache = make_cache()
    assert await cache.check_or_insert(KEY, "job-pending-here", "gate-b") == (False, None)
    waiter = asyncio.ensure_future(cache.check_or_insert(KEY, "job-pending-here", "gate-b"))
    await asyncio.sleep(0)

    # This gate's own job: its commit will record the full answer.
    await cache.adopt_committed(KEY, "job-pending-here", "gate-b")
    assert (await cache.get(KEY)).status == IdempotencyStatus.PENDING

    # Another gate's job decided the key: the pending submission is
    # answered with it.
    await cache.adopt_committed(KEY, "job-original", "gate-a")
    found, entry = await asyncio.wait_for(waiter, timeout=1.0)
    assert found and (entry.status, entry.job_id) == (IdempotencyStatus.COMMITTED, "job-original")


@pytest.mark.asyncio
async def test_a_retry_at_a_gate_that_holds_the_replica_is_answered_for_the_original_job() -> None:
    """The accepting gate died after acknowledging; the retry under the key
    lands on a gate whose cache only adopted the key from the replica."""
    cache = make_cache()
    await cache.adopt_committed(KEY, "job-original", "gate-a")
    handler = create_mock_handler(idempotency_cache=cache)

    retry = JobAck.load(
        await handler.handle_submission(
            addr=("10.0.0.50", 8500),
            data=JobSubmission(
                job_id="job-retried-under-a-fresh-id",
                workflows=WORKFLOWS,
                vus=1,
                timeout_seconds=60.0,
                datacenter_count=1,
                idempotency_key=str(KEY),
            ).dump(),
            active_gate_peer_count=2,
        )
    )

    assert retry.accepted and retry.was_duplicate
    assert retry.job_id == retry.original_job_id == "job-original"
