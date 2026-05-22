"""
Watch-stream client (AD-52 §9).

Long-lived consumer of the watch stream. Maintains the local soft-state
cache by applying received deltas + snapshots. Handles reconnect-and-
resume against arbitrary server-side failures (lost connection,
catastrophic snapshot due to ring-buffer overrun).

This module owns the consumer state machine. It does NOT own the wire-
level connection — see WatchSource below for the abstraction the
transport layer fills in.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from .models.watch_messages import (
    WatchDelta,
    WatchFilter,
    WatchOpen,
    WatchSnapshot,
)

if TYPE_CHECKING:
    from hyperscale.logging import Logger


@runtime_checkable
class WatchSource(Protocol):
    """One open watch stream. The transport implements this; the client
    consumes it. Returning None from next_message() means the stream is
    closed and should be reconnected."""

    async def send_watch_open(self, watch_open: WatchOpen) -> None:
        ...

    async def next_message(self) -> WatchSnapshot | WatchDelta | None:
        ...

    async def close(self) -> None:
        ...


class WatchClient:
    """
    Drives reconnect-and-resume against a WatchSource provider.

    Lifecycle:
      run()  : start the consume loop. Reconnects on disconnect.
      stop() : signal the loop to exit cleanly.
    """

    __slots__ = (
        "_watcher_node_id",
        "_filter_spec",
        "_open_source",
        "_on_snapshot",
        "_on_delta",
        "_logger",
        "_last_seen_lsn",
        "_last_seen_epoch",
        "_stop_event",
        "_reconnect_backoff_initial_seconds",
        "_reconnect_backoff_max_seconds",
    )

    def __init__(
        self,
        watcher_node_id: str,
        filter_spec: WatchFilter,
        open_source: Callable[[], Awaitable[WatchSource]],
        on_snapshot: Callable[[WatchSnapshot], Awaitable[None]],
        on_delta: Callable[[WatchDelta], Awaitable[None]],
        logger: "Logger | None" = None,
        reconnect_backoff_initial_seconds: float = 0.5,
        reconnect_backoff_max_seconds: float = 30.0,
    ) -> None:
        self._watcher_node_id = watcher_node_id
        self._filter_spec = filter_spec
        self._open_source = open_source
        self._on_snapshot = on_snapshot
        self._on_delta = on_delta
        self._logger = logger
        self._last_seen_lsn: int = 0
        self._last_seen_epoch: int = 0
        self._stop_event = asyncio.Event()
        self._reconnect_backoff_initial_seconds = reconnect_backoff_initial_seconds
        self._reconnect_backoff_max_seconds = reconnect_backoff_max_seconds

    async def run(self) -> None:
        """Consume loop. Returns when stop() is called."""
        backoff_seconds = self._reconnect_backoff_initial_seconds
        while not self._stop_event.is_set():
            try:
                source = await self._open_source()
            except Exception as open_error:
                await self._log_error("WatchClientOpenFailed", str(open_error))
                if await self._wait_or_stop(backoff_seconds):
                    return
                backoff_seconds = min(
                    backoff_seconds * 2.0,
                    self._reconnect_backoff_max_seconds,
                )
                continue

            try:
                await source.send_watch_open(
                    WatchOpen(
                        last_seen_membership_epoch=self._last_seen_epoch,
                        last_seen_lsn=self._last_seen_lsn,
                        watcher_node_id=self._watcher_node_id,
                        watch_filter=self._filter_spec,
                    )
                )
                # Connected: reset backoff.
                backoff_seconds = self._reconnect_backoff_initial_seconds
                await self._consume_until_closed(source)
            except Exception as consume_error:
                await self._log_error("WatchClientConsumeFailed", str(consume_error))
            finally:
                try:
                    await source.close()
                except Exception:
                    pass

            # Loop will reconnect — possibly resuming from
            # _last_seen_lsn if still within the server's ring buffer.

    async def stop(self) -> None:
        self._stop_event.set()

    async def _consume_until_closed(self, source: WatchSource) -> None:
        while not self._stop_event.is_set():
            message = await source.next_message()
            if message is None:
                return  # Stream closed; reconnect loop reopens.
            if isinstance(message, WatchSnapshot):
                self._last_seen_epoch = message.snapshot_epoch
                self._last_seen_lsn = message.snapshot_lsn
                await self._on_snapshot(message)
            elif isinstance(message, WatchDelta):
                self._last_seen_epoch = message.epoch
                self._last_seen_lsn = message.lsn
                await self._on_delta(message)

    async def _wait_or_stop(self, seconds: float) -> bool:
        """Sleep up to `seconds`. Returns True if stop was signaled."""
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False

    async def _log_error(self, event_name: str, error_message: str) -> None:
        if self._logger is None:
            return
        await self._logger.log(
            {
                "event": event_name,
                "watcher_node_id": self._watcher_node_id,
                "error": error_message,
            }
        )
