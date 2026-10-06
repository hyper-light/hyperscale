"""
``Context`` per-key locks live only while a lease holds or awaits them.

SWIM serializes its handling of each peer -- join, leave, probe, ping-req --
under ``Context.with_value(peer)``. Each peer address ever seen used to
keep its lock forever, and probe/ping-req targets come off the network, so
the map grew without bound. The lease keeps the section's semantics --
mutual exclusion per key, reentrancy within a task -- and the last lease
out removes the key.
"""

from __future__ import annotations

import asyncio

import pytest

from hyperscale.distributed.server.context import Context

PEER_COUNT = 1000


@pytest.mark.asyncio
async def test_a_section_per_peer_leaves_no_lock_behind() -> None:
    context: Context = Context()

    for port in range(PEER_COUNT):
        async with await context.with_value(("10.0.0.1", port)):
            pass

    assert context._value_locks == {}
    assert context._value_lock_users == {}


@pytest.mark.asyncio
async def test_sections_on_one_key_exclude_each_other_and_then_clean_up() -> None:
    context: Context = Context()
    peer = ("10.0.0.1", 9000)
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    order: list[str] = []

    async def first_section() -> None:
        async with await context.with_value(peer):
            order.append("first-in")
            first_entered.set()
            await release_first.wait()
            order.append("first-out")

    async def second_section() -> None:
        await first_entered.wait()
        async with await context.with_value(peer):
            order.append("second-in")

    first = asyncio.ensure_future(first_section())
    second = asyncio.ensure_future(second_section())
    await first_entered.wait()
    await asyncio.sleep(0)
    assert order == ["first-in"]
    assert context._value_lock_users[peer] == 2

    release_first.set()
    await asyncio.gather(first, second)

    assert order == ["first-in", "first-out", "second-in"]
    assert context._value_locks == {}


@pytest.mark.asyncio
async def test_a_write_inside_its_keys_section_reenters_it() -> None:
    context: Context = Context()

    async with await context.with_value("current_timeout"):
        await context.write("current_timeout", 1.5)
        assert await context.update("current_timeout", lambda value: (value or 0.0) * 2) == 3.0

    assert await context.read("current_timeout") == 3.0
    assert context._value_locks == {}


@pytest.mark.asyncio
async def test_a_waiter_cancelled_before_it_entered_gives_its_claim_back() -> None:
    context: Context = Context()
    peer = ("10.0.0.1", 9000)
    holder_entered = asyncio.Event()
    release_holder = asyncio.Event()

    async def holder() -> None:
        async with await context.with_value(peer):
            holder_entered.set()
            await release_holder.wait()

    async def waiter() -> None:
        async with await context.with_value(peer):
            raise AssertionError("a cancelled waiter must never enter")

    holding = asyncio.ensure_future(holder())
    await holder_entered.wait()
    waiting = asyncio.ensure_future(waiter())
    await asyncio.sleep(0)
    assert context._value_lock_users[peer] == 2

    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert context._value_lock_users[peer] == 1

    release_holder.set()
    await holding
    assert context._value_locks == {}
    assert context._value_lock_users == {}
