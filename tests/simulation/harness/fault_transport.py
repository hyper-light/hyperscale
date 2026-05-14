"""
FaultInjectingTransport — wraps every harness-managed server's
``send_tcp`` and ``send_udp`` methods at the instance level so the
``FaultMatrix`` can drop, delay, partition, throttle, duplicate, reorder,
or reset messages between specific (src, dst) pairs without touching
production code.

REAL-mode wiring: Python's bound-method assignment lets us replace
the methods on a per-instance basis without subclassing or
monkey-patching the class. The wrapper closes over a reference to
the harness's ``FaultMatrix`` and the harness's address-to-node-id
maps, consults the matrix's rules on every send, and:

* Returns a synthetic ``(asyncio.TimeoutError, clock)`` tuple if the
  pair is partitioned or hits a drop_rate roll. The shape matches
  the original methods' on-error return contract so production code
  consuming the result sees an indistinguishable timeout.
* Sleeps for a configured delay (with optional jitter) before
  forwarding. Real-network delay simulation.
* Adds token-bucket bandwidth delay and UDP-style reordering holds at
  the same point, before the original send method.
* Delivers bounded duplicate copies by invoking the original send method
  for extra copies with a short timeout, so duplicate delivery never
  creates an orphaned background task.
* Closes the cached TCP transport and returns ``ConnectionResetError``
  for configured TCP-reset rules.
* Otherwise forwards to the original bound method.

The address-to-node-id resolution uses the harness's ServerHandle
catalog: every harness-managed server has known TCP and UDP ports,
so dst lookup is O(1). External clients (HyperscaleClient ports) are
not in the catalog — sends to them are passed through unchanged.

This module is REAL-mode only. Phase 6 SIM mode replaces the entire
transport layer with an in-process scheduler-driven implementation
that consults the same FaultMatrix instance through a different
interface; scenarios written against ``FaultMatrix.partition`` /
``delay`` / ``drop_rate`` work in both modes unchanged.
"""

from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING, Any, Awaitable, Callable

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness
    from tests.simulation.harness.fault_matrix import FaultMatrix
    from tests.simulation.harness.server_handle import ServerHandle


# Type aliases for clarity.
SendMethod = Callable[..., Awaitable[tuple[Any, int]]]


def install(harness: "ClusterHarness") -> None:
    """Install the fault-injecting wrappers on every server in the
    harness.

    Idempotent: calling twice on the same server is a no-op (the
    wrapper checks for a sentinel attribute set on first install).

    Must be called *after* ``ServerHandle.instance`` is populated and
    started — the wrapper closes over the bound original methods.
    Restart() in the FaultMatrix re-installs after rebuild because
    the new instance has fresh unwrapped methods.
    """
    for handle in harness.all_handles():
        _install_one(harness, handle)


def reinstall_for(handle: "ServerHandle", harness: "ClusterHarness") -> None:
    """Re-install the wrapper on a single restarted handle. Called
    by ``FaultMatrix.restart`` after ``handle.instance`` has been
    rebuilt and started."""
    _install_one(harness, handle)


def _install_one(harness: "ClusterHarness", handle: "ServerHandle") -> None:
    instance = handle.instance
    if getattr(instance, "_fault_transport_installed", False):
        return

    original_send_tcp: SendMethod = instance.send_tcp
    original_send_udp: SendMethod = instance.send_udp

    src_node_id = handle.node_id
    matrix_provider: Callable[[], FaultMatrix] = lambda: harness.faults
    rng = random.Random(hash(src_node_id) & 0xFFFFFFFF)

    async def send_tcp_wrapped(
        address: tuple[str, int],
        action: str,
        data: Any,
        timeout: int | float | None = None,
    ) -> tuple[Any, int]:
        return await _send_with_faults(
            kind="tcp",
            harness=harness,
            matrix=matrix_provider(),
            src_node_id=src_node_id,
            address=address,
            action=action,
            data=data,
            timeout=timeout,
            original=original_send_tcp,
            rng=rng,
            instance=instance,
        )

    async def send_udp_wrapped(
        address: tuple[str, int],
        action: str,
        data: Any,
        timeout: int | float | None = None,
    ) -> tuple[Any, int]:
        return await _send_with_faults(
            kind="udp",
            harness=harness,
            matrix=matrix_provider(),
            src_node_id=src_node_id,
            address=address,
            action=action,
            data=data,
            timeout=timeout,
            original=original_send_udp,
            rng=rng,
            instance=instance,
        )

    instance.send_tcp = send_tcp_wrapped  # type: ignore[method-assign]
    instance.send_udp = send_udp_wrapped  # type: ignore[method-assign]
    instance._fault_transport_installed = True
    instance._fault_transport_originals = (original_send_tcp, original_send_udp)


def uninstall(handle: "ServerHandle") -> None:
    """Restore the original ``send_tcp`` / ``send_udp`` methods on a
    handle. Called when a handle is torn down so the supervisor's
    cleanup doesn't see ghost wrappers."""
    instance = handle.instance
    originals = getattr(instance, "_fault_transport_originals", None)
    if originals is None:
        return
    original_tcp, original_udp = originals
    instance.send_tcp = original_tcp
    instance.send_udp = original_udp
    instance._fault_transport_installed = False
    delattr(instance, "_fault_transport_originals")


async def _send_with_faults(
    *,
    kind: str,
    harness: "ClusterHarness",
    matrix: "FaultMatrix",
    src_node_id: str,
    address: tuple[str, int],
    action: str,
    data: Any,
    timeout: int | float | None,
    original: SendMethod,
    rng: random.Random,
    instance: Any,
) -> tuple[Any, int]:
    """Apply partition / drop / delay rules then forward to the original.

    The clock returned with synthetic errors is the instance's own
    clock so production callers see a monotonically advancing
    timestamp regardless of fault path. Production code does not
    depend on any particular value here beyond it being a non-zero
    integer; the test layer just preserves the shape.
    """
    dst_node_id = harness.address_to_node_id(address, kind=kind)

    if dst_node_id is not None and matrix.is_partitioned(
        src_node_id, dst_node_id
    ):
        return _synthetic_timeout(instance, kind, "partitioned")

    if dst_node_id is not None:
        drop_p = matrix.drop_probability(src_node_id, dst_node_id)
        if drop_p > 0.0 and rng.random() < drop_p:
            return _synthetic_timeout(instance, kind, "dropped")

        if kind == "tcp" and matrix.should_reset_tcp(
            src_node_id,
            dst_node_id,
            action,
            rng,
        ):
            _close_cached_tcp_transport(instance, address)
            return _synthetic_reset(instance, action)

        delay_seconds = matrix.delay_seconds(src_node_id, dst_node_id, rng)
        delay_seconds += matrix.reorder_delay_seconds(
            kind,
            src_node_id,
            dst_node_id,
            rng,
        )
        delay_seconds += matrix.bandwidth_delay_seconds(
            src_node_id,
            dst_node_id,
            _estimate_payload_size(action, data),
        )
        if delay_seconds > 0.0:
            await asyncio.sleep(delay_seconds)

        duplicate_count = matrix.duplicate_count(
            kind,
            src_node_id,
            dst_node_id,
            rng,
        )
        result = await original(address, action, data, timeout=timeout)
        for _duplicate_index in range(duplicate_count):
            await original(address, action, data, timeout=0.05)
        return result

    return await original(address, action, data, timeout=timeout)


def _synthetic_timeout(
    instance: Any, kind: str, reason: str
) -> tuple[Any, int]:
    """Return the on-error tuple the original would produce on
    timeout — preserves the production-side error-handling shape."""
    clock_attr = "_tcp_clock" if kind == "tcp" else "_udp_clock"
    clock = getattr(instance, clock_attr, None)
    clock_value = getattr(clock, "time", 0)
    return (asyncio.TimeoutError(reason), clock_value)


def _synthetic_reset(instance: Any, action: str) -> tuple[Any, int]:
    """Return the on-error tuple for a configured TCP reset."""
    clock = getattr(instance, "_tcp_clock", None)
    clock_value = getattr(clock, "time", 0)
    return (ConnectionResetError(f"tcp reset during {action}"), clock_value)


def _close_cached_tcp_transport(instance: Any, address: tuple[str, int]) -> None:
    """Abort the cached TCP transport for ``address`` if one exists."""
    transports = getattr(instance, "_tcp_client_transports", None)
    if not isinstance(transports, dict):
        return
    transport = transports.pop(address, None)
    if transport is None or transport.is_closing():
        return
    abort = getattr(transport, "abort", None)
    if callable(abort):
        abort()
        return
    transport.close()


def _estimate_payload_size(action: str, data: Any) -> int:
    """Estimate application payload size before production framing/encryption."""
    if isinstance(data, bytes):
        return len(data) + len(action)
    if isinstance(data, bytearray | memoryview):
        return len(data) + len(action)
    dump = getattr(data, "dump", None)
    if callable(dump):
        try:
            dumped = dump()
        except Exception:
            dumped = None
        if isinstance(dumped, bytes):
            return len(dumped) + len(action)
    return len(repr(data).encode("utf-8")) + len(action)
