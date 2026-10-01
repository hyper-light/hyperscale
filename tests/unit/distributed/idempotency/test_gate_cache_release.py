"""
GateIdempotencyCache.release: a PENDING submission that ended without an
outcome worth replaying (a transient refusal) is forgotten, so the
client's retry with the same key is decided afresh.

Before release existed, a gate submission that returned a transient
refusal left its key PENDING: the client's retries waited on it, and a
waiter whose wait timed out went on to admit the job -- after the client
had given up and resubmitted (measured: the job executed twice).

Pinned against the real cache:

* release wakes every waiter at once (no result) and forgets the entry,
  so the next check_or_insert inserts afresh;
* release never touches a committed or rejected entry -- a decided
  outcome is still replayed;
* releasing an unknown key is a no-op.
"""

import asyncio

import pytest

from hyperscale.distributed.idempotency.gate_cache import GateIdempotencyCache
from hyperscale.distributed.idempotency.idempotency_config import IdempotencyConfig
from hyperscale.distributed.idempotency.idempotency_key import IdempotencyKey
from hyperscale.distributed.idempotency.idempotency_status import IdempotencyStatus

KEY = IdempotencyKey(client_id="client-1", sequence=1, nonce="nonce-1")


def make_cache() -> GateIdempotencyCache[bytes]:
    return GateIdempotencyCache(IdempotencyConfig(), task_runner=None, logger=None)


@pytest.mark.asyncio
async def test_release_wakes_waiters_and_the_next_attempt_inserts_afresh() -> None:
    cache = make_cache()
    assert await cache.check_or_insert(KEY, "job-1", "gate-a") == (False, None)

    waiter = asyncio.ensure_future(cache.check_or_insert(KEY, "job-1", "gate-b"))
    await asyncio.sleep(0)
    assert not waiter.done()

    await cache.release(KEY)
    found, entry = await asyncio.wait_for(waiter, timeout=1.0)

    assert (found, entry) == (True, None)
    assert await cache.check_or_insert(KEY, "job-1", "gate-a") == (False, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("decide", ["commit", "reject"])
async def test_release_never_undoes_a_decided_outcome(decide: str) -> None:
    cache = make_cache()
    await cache.check_or_insert(KEY, "job-1", "gate-a")
    await getattr(cache, decide)(KEY, b"decided")

    await cache.release(KEY)

    found, entry = await cache.check_or_insert(KEY, "job-1", "gate-a")
    assert found and entry is not None and entry.result == b"decided"
    expected = IdempotencyStatus.COMMITTED if decide == "commit" else IdempotencyStatus.REJECTED
    assert entry.status == expected


@pytest.mark.asyncio
async def test_releasing_an_unknown_key_is_a_no_op() -> None:
    cache = make_cache()
    await cache.release(KEY)
    assert await cache.check_or_insert(KEY, "job-1", "gate-a") == (False, None)
